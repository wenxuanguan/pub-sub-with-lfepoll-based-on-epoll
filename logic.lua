function explode(source,symbol)
	index=string.find(source,symbol)
	if(not index) then
		return index
	else
		source=string.gsub(source,"\r\n","")
		res=string.sub(source,1,index-1)
		source=string.sub(source,index+1)
	end
	return res,source
end


local sfd =lua_ctx:getSfd()
local request = lua_ctx:getRequest()
--lua_ctx:setResponse("sfd:\r\n" .. sfd);
--lua_ctx:setResponse("lua_extension myechos:\r\n " .. request);
local cmd,data=explode(request,"|")

local redis=require 'redis'
local client=redis.connect('127.0.0.1',6379)
if(not cmd)then
	lua_ctx:setResponse("invalid cmd:" .. request.."\r\n");
elseif(string.match(cmd , 'subscribe')) then
	client:sadd(data,sfd)
	client:sadd(sfd,data)
	lua_ctx:setResponse("sub:channel:"..data.."sfd:" .. sfd.."\r\n");
elseif(string.match(cmd,'publish'))then
	local channel,p_data= explode(data,"|")
	if(not channel)then
		lua_ctx:setResponse("publish: invalid data:"..data.."\r\n");		
	else
		local c=client:smembers(channel)
		for k,v in pairs(c)do
			lua_ctx:publish("get published data:" .. p_data .. "\r\n", v);
		end
--		lua_ctx:setResponse("publish: channel:"..channel.." data:" ..p_data.."\r\n");
	end


else
	lua_ctx:setResponse("invalid cmd:" .. cmd.."\r\n");

end


