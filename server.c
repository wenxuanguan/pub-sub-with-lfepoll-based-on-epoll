#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <pthread.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <signal.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <time.h>

#ifdef SUPPORT_LUA_5_2
    #include <lua.h>
    #include <lauxlib.h>
    #include <lualib.h>
#endif

typedef struct connection_st *connection_t;

#ifdef SUPPORT_LUA_5_2
typedef struct lua_ctx_st {
    #define LUA_BUF_SIZE 4096
    int  reqLength;
    const char *request; 
    connection_t conn;
}*lua_ctx_t;

char *luaCode = NULL;

int  readLuaCode(const char *file);
int initLuaState(connection_t conn);
void freeLuaState(connection_t conn);
int  runLuaExtension(connection_t conn, const char *req, int size);

int luaGetRequest(lua_State *L);
int luaSetResponse(lua_State *L);
int luaGetSfd(lua_State *L);
int luaPublish(lua_State *L);


struct luaL_Reg lua_api_table[] = {
    {"getRequest" , luaGetRequest}, 
    {"setResponse", luaSetResponse},
	{"getSfd", luaGetSfd},
	{"publish", luaPublish},

};
#endif

struct connection_st {
    int sock;
    int index; /* which epoll fd this conn belongs to*/
    int using;
#define BUF_SIZE 4096
    int roff;
    char rbuf[BUF_SIZE];
    int woff;
    char wbuf[BUF_SIZE];
#ifdef SUPPORT_LUA_5_2
    lua_State *L;
    lua_ctx_t ctx;
    int r; /*  the index in REGISTRY table of lua function */
#endif 
};

//#define CONN_MAXFD 65536
#define CONN_MAXFD 1024

struct connection_st g_conn_table[CONN_MAXFD] = {0};

static sig_atomic_t shut_server = 0;

void shut_server_handler(int signo) {
    shut_server = 1;
}

#define EPOLL_NUM 10
int epfd[EPOLL_NUM];
int lisSock;

#define WORKER_PER_GROUP 5

#define NUM_WORKER (EPOLL_NUM * WORKER_PER_GROUP)
pthread_t worker[NUM_WORKER]; /* echo group has 5 worker threads */

int sendData(connection_t conn, const char *data, int len) {
    if (conn->woff){
        if (conn->woff + len > BUF_SIZE) {
            return -1;
        }
        memcpy(conn->wbuf + conn->woff, data, len);
        conn->woff += len;
        return 0;
    } else {
        int ret = write(conn->sock, data, len);
        if (ret > 0){
            if (ret == len) {
                return 0;
            }
            int left = len - ret;
            if (left > BUF_SIZE) return -1;
            
            memcpy(conn->wbuf, data + ret, left);
            conn->woff = left;
        } else {
            if (errno != EINTR && errno != EAGAIN) {
                return -1;
            }
            if (len > BUF_SIZE) {
                return -1;
            }
            memcpy(conn->wbuf, data, len);
            conn->woff = len;
        }
    }

    return 0;
}

int handleReadEvent(connection_t conn) {
    if (conn->roff == BUF_SIZE) {
        return -1;
    }
    
    int ret = read(conn->sock, conn->rbuf + conn->roff, BUF_SIZE - conn->roff);

    if (ret > 0) {
        conn->roff += ret;
        
        int beg, end, len;
        beg = end = 0;
        while (beg < conn->roff) {
            char *endPos = (char *)memchr(conn->rbuf + beg, '\n', conn->roff - beg);
            if (!endPos) break;
            end = endPos - conn->rbuf;
            len = end - beg + 1;
            
            /* service's logic handler, either for lua5.2 or for pure c */
#ifdef SUPPORT_LUA_5_2
            if (runLuaExtension(conn, conn->rbuf + beg, len) == -1) { 
                printf("runLuaExtension=-1 socket=%d\n", conn->sock);
                return -1;
            }
#else
            if (sendData(conn, conn->rbuf + beg, len) == -1) return -1;
#endif
            printf("request_finish_time=%d\n", time(NULL));
            beg = end + 1;
        }
        int left = conn->roff - beg;
        if (beg != 0 && left > 0) {
            memmove(conn->rbuf, conn->rbuf + beg, left);
        }
        conn->roff = left;
    } else if (ret == 0) {
        return -1;
    } else {
        if (errno != EINTR && errno != EAGAIN) {
            return -1;
        }
    }

    return 0;
}

int handleWriteEvent(connection_t conn) {
    if (conn->woff == 0) return 0;

    int ret = write(conn->sock, conn->wbuf, conn->woff);

    if (ret == -1) {
        if (errno != EINTR && errno != EAGAIN) {
            return -1;
        }
    } else {
        int left = conn->woff - ret;

        if (left > 0) {
            memmove(conn->wbuf, conn->wbuf + ret, left);
        }

        conn->woff = left;
    }

    return 0;
}

void closeConnection(connection_t conn) {
    struct epoll_event evReg;

    conn->using = 0;
    conn->woff = conn->roff = 0;
    epoll_ctl(epfd[conn->index], EPOLL_CTL_DEL, conn->sock, &evReg);
    close(conn->sock);
}

void *workerThread(void *arg) {
    int epfd = *(int *)arg;
    
    struct epoll_event event;
    struct epoll_event evReg;

    /* only handle connected socket */
    while (!shut_server) {
        int numEvents = epoll_wait(epfd, &event, 1, 1000);
        
        if (numEvents > 0) {
            int sock = event.data.fd;
            connection_t conn = &g_conn_table[sock];
                
            if (event.events & EPOLLOUT) {
                if (handleWriteEvent(conn) == -1) {
                    closeConnection(conn);
                    continue;
                }
            }

            if (event.events & EPOLLIN) {
                if (handleReadEvent(conn) == -1) {
                    closeConnection(conn);
                    continue;
                }
            }

            evReg.events = EPOLLIN | EPOLLONESHOT;
            if (conn->woff > 0) evReg.events |= EPOLLOUT;
            evReg.data.fd = sock;
            epoll_ctl(epfd, EPOLL_CTL_MOD, conn->sock, &evReg);
        }
    }
    return NULL;
}

void *listenThread(void *arg) {
    int lisEpfd = epoll_create(5);

    struct epoll_event evReg;
    evReg.events  = EPOLLIN;
    evReg.data.fd = lisSock;

    epoll_ctl(lisEpfd, EPOLL_CTL_ADD, lisSock, &evReg);
    
    struct epoll_event event;

    int rrIndex = 0; /* round robin index */
    
    /* only handle listen socekt */
    while (!shut_server) {
        int numEvent = epoll_wait(lisEpfd, &event, 1, 1000);
        if (numEvent > 0) {
            int sock = accept(lisSock, NULL, NULL);
            if (sock > 0) {
                g_conn_table[sock].using = 1;
                    
                int flag;
                flag = fcntl(sock, F_GETFL);
                fcntl(sock, F_SETFL, flag | O_NONBLOCK);
                    
                evReg.data.fd = sock;
                evReg.events = EPOLLIN | EPOLLONESHOT;
                            
                /* register to worker-pool's epoll,
                 * not the listen epoll */
                g_conn_table[sock].index= rrIndex;
                epoll_ctl(epfd[rrIndex], EPOLL_CTL_ADD, sock, &evReg);
                rrIndex = (rrIndex + 1) % EPOLL_NUM;
            }
        }
    }

    close(lisEpfd);
    return NULL;
}

int main(int argc, char *const argv[]) {
#ifdef SUPPORT_LUA_5_2
    if (readLuaCode("logic.lua") == -1) return -1;
#endif
    
    int c;
    for (c = 0; c < CONN_MAXFD; ++c) {
        g_conn_table[c].sock = c;
#ifdef SUPPORT_LUA_5_2
        if (initLuaState(g_conn_table + c) == -1) {
            int i;
            for (i = 0; i < c; ++i) {
                freeLuaState(g_conn_table + i);
            }
            return -1;
        }
#endif
    }
    struct sigaction act;
    memset(&act, 0, sizeof(act));

    act.sa_handler = shut_server_handler;
    sigaction(SIGINT, &act, NULL);
    sigaction(SIGTERM, &act, NULL);

    /* create EPOLL_NUM different epoll fd */
    int epi;
    for (epi = 0; epi < EPOLL_NUM; ++ epi) {
        epfd[epi] = epoll_create(20);
    }
    
    lisSock = socket(AF_INET, SOCK_STREAM, 0); 
    
    int reuse = 1;
    setsockopt(lisSock, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
    
    int flag;
    flag = fcntl(lisSock, F_GETFL);
    fcntl(lisSock, F_SETFL, flag | O_NONBLOCK);

    struct sockaddr_in lisAddr;
    lisAddr.sin_family = AF_INET;
    lisAddr.sin_port = htons(9876);
    lisAddr.sin_addr.s_addr = htonl(INADDR_ANY);
    
    if (bind(lisSock, (struct sockaddr *)&lisAddr, sizeof(lisAddr)) == -1) {
        perror("bind");
        return -1;
    }

    listen(lisSock, 4096);
    
    pthread_t lisTid;
    pthread_create(&lisTid, NULL, listenThread, NULL);

    int i;

    for (i = 0; i < EPOLL_NUM; ++i) {
        int j;
        for (j = 0; j < WORKER_PER_GROUP; ++j) {
            pthread_create(worker + (i * WORKER_PER_GROUP + j), NULL, workerThread, epfd + i);
        }
    }
    
    for (i = 0; i < NUM_WORKER; ++i) {
        pthread_join(worker[i], NULL);
    }

    pthread_join(lisTid, NULL);
    
    struct epoll_event evReg;

    for (c = 0; c < CONN_MAXFD; ++c) {
        connection_t conn = g_conn_table + c;
        if (conn->using) { 
            epoll_ctl(epfd[conn->index], EPOLL_CTL_DEL, conn->sock, &evReg);
            close(conn->sock);
        }
#ifdef SUPPORT_LUA_5_2
        freeLuaState(conn);
#endif
    } 
#ifdef SUPPORT_LUA_5_2   
    free(luaCode);
#endif

    for (epi = 0; epi < EPOLL_NUM; ++epi) {
        close(epfd[epi]);
    }
    close(lisSock);

    return 0;
}

#ifdef SUPPORT_LUA_5_2
int readLuaCode(const char *file) {
    if (!file) return -1;

    struct stat stbuf;
    if (stat(file, &stbuf) == -1) {
        return -1;
    }
    
    luaCode = calloc(1, stbuf.st_size + 1);
    if (!luaCode) return -1;
    
    int fd = open(file, O_RDONLY);
    if (fd == -1) {
        free(luaCode);   
        return -1;
    }
    int ret = read(fd, luaCode, stbuf.st_size);
    if (ret != stbuf.st_size) { 
        free(luaCode);
        close(fd);
        return -1;
    }
    luaCode[stbuf.st_size] = '\0';
    return 0;
}

int luaGetRequest(lua_State *L) { /* working in thread */
    lua_ctx_t ctx = luaL_checkudata(L, 1, "lua_extension");
    if (!ctx) {
        lua_pushstring(L, "invalid userdata");
        lua_error(L);
    }
    if (!lua_pushlstring(L, ctx->request, ctx->reqLength)) {
        lua_pushstring(L, "lack of memory");
        lua_error(L);
    }
    return 1;
}

int luaGetSfd(lua_State *L) { /* working in thread */
    lua_ctx_t ctx = luaL_checkudata(L, 1, "lua_extension");
    if (!ctx) {
        lua_pushstring(L, "invalid userdata");
        lua_error(L);
    }
    lua_pushinteger(L, ctx->conn->sock);
    return 1;
}

int luaSetResponse(lua_State *L) { /* working in thread */
    lua_ctx_t ctx = luaL_checkudata(L, 1, "lua_extension");
    if (!ctx) {
        lua_pushstring(L, "invalid userdata");
        lua_error(L);
    }
    size_t len;
    const char *res = luaL_checklstring(L, 2, &len);
    if (!res) {
        lua_pushstring(L, "param #1 error, call lua_ctx:setResponse(res)");
        lua_error(L);
    }
    if (sendData(ctx->conn, res, len) == -1) {
        lua_pushstring(L, "sendData failed");
        lua_error(L);
    }
    return 0;
}

int luaPublish(lua_State *L) { /* working in thread */
    lua_ctx_t ctx = luaL_checkudata(L, 1, "lua_extension");
    if (!ctx) {
        lua_pushstring(L, "invalid userdata");
        lua_error(L);
    }
    size_t len;
    const char *res = luaL_checklstring(L, 2, &len);
    if (!res) {
        lua_pushstring(L, "param #1 error, call lua_ctx:setResponse(res)");
        lua_error(L);
    }
	int socket_fd=luaL_checkinteger(L,3);
int length=send(socket_fd, res, len,0);
    if (length!=len) {
        lua_pushstring(L, "sendData failed457");
        lua_error(L);
    }
    return 0;
}


int initLuaState(connection_t conn) {
    conn->L   = luaL_newstate();
    luaL_requiref(conn->L,"string",luaopen_string,1); 
	lua_pop(conn->L, 1);
    luaL_requiref(conn->L, "math", luaopen_math, 1);
	lua_pop(conn->L, 1);

    luaL_requiref(conn->L, "base", luaopen_base, 1);
	lua_pop(conn->L, 1);
	luaL_requiref(conn->L, "table", luaopen_table, 1);
	lua_pop(conn->L, 1);
    luaL_requiref(conn->L, "package", luaopen_package, 1);
    conn->ctx = lua_newuserdata(conn->L, sizeof(struct lua_ctx_st));
    conn->ctx->conn = conn;
    
    luaL_newmetatable(conn->L, "lua_extension"); /* create metatable for userdata*/
    lua_pushvalue(conn->L, -1); /* dup a userdata */
    lua_setfield(conn->L, -2, "__index"); /* set userdata's metatable's "__index" = metatable itself */
    luaL_setfuncs(conn->L, lua_api_table, 0); /* register member function for lua_ctx in metatable */
    lua_setmetatable(conn->L, 2); /* set metatable for userdata */
    lua_setglobal(conn->L, "lua_ctx"); /* set userdata as global var "lua_ctx" */
    
    if (luaL_loadstring(conn->L, luaCode) != LUA_OK) { /* load chunk into main thread*/
        printf("luaL_loadstring!=LUA_OK\n");
        return -1;
    }
    conn->r = luaL_ref(conn->L, LUA_REGISTRYINDEX); /* register lua func for future use*/
    return 0;
}

void freeLuaState(connection_t conn) {
    lua_close(conn->L);
}

int runLuaExtension(connection_t conn, const char *req, int size) {
    lua_ctx_t ctx = conn->ctx;
    ctx->request = req;
    ctx->reqLength = size;

    lua_State *LL = lua_newthread(conn->L); /* create new thread */
    lua_rawgeti(LL, LUA_REGISTRYINDEX, conn->r);
    
    if (lua_pcall(LL, 0, 0, 0) != LUA_OK) { /*excute the lua script*/
        const char *errmsg = lua_tostring(LL, -1);
        printf("lua_errmsg=%s\n", errmsg);
        lua_pop(conn->L, 1); /* gc the thread*/
        return -1;
    }
    lua_pop(conn->L, 1); /* destroy the thread */
    return 0;
}
#endif
