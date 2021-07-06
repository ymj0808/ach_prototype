local id = ngx.var.arg_id
local size = tonumber(ngx.var.arg_size)

local str = '#'
local count = 1

while true do
	if count >= size then
		break
	else
		str = str..str
		count = count * 2
	end
end

ngx.say(string.sub(str, 1, size))