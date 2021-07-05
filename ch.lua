--- consistent hash ---

local _M = {}                 -- module of consistent
local mt = { __index = _M }

function table_len(t)
  local len=0
  for k, v in pairs(t) do
    len=len+1
  end
  return len;
end

local crc32 = function(arg)            -- crc32 algorithm
    return math.abs(ngx.crc32_long(arg))
    --return math.abs(tonumber(arg) * 1234321 % 999)
end

function add_rnode_ch(rnode_ch, vnode_ch, vnode_ch_num, index, ip)
    local tmp_rnode_ch_index = index
    local tmp_rnode_ch_ip = ip
    local tmp_rnode_ch_hash_val = crc32(tmp_rnode_ch_ip)
    table.insert(rnode_ch, {ip, tmp_rnode_ch_index, tmp_rnode_ch_hash_val})

    for i = 1, vnode_ch_num do
        --local tmp_vnode_ch_ip = tmp_rnode_ch_ip..'-'..i
        local tmp_vnode_ch_ip = tmp_rnode_ch_ip..i
        local tmp_vnode_ch_hash_val = crc32(tmp_vnode_ch_ip)
        local tmp_vnode_ch_index = i
        table.insert(vnode_ch, {tmp_vnode_ch_index, tmp_rnode_ch_index, tmp_vnode_ch_hash_val})
    end
end

-- vnode_ch--element = {vnode_ch_index, rnode_ch_index, hash_val, ***}
function hash_find_ch(self, key, lo, hi)
    print("ch hashfind")
    if key <= self.vnode_ch[lo][3] or key > self.vnode_ch[hi][3] then
        return self.vnode_ch[lo][1]
    end

    local middle = lo + math.floor((hi - lo) / 2)
    if middle == 1 then
        return self.vnode_ch[middle][1]
    elseif key <=self.vnode_ch[middle][3] and key > self.vnode_ch[middle-1][3] then
        return self.vnode_ch[middle][1]
    elseif key > self.vnode_ch[middle][3] then
        return hash_find_ch(self, key, middle+1, hi)
    end
    return hash_find_ch(self, key, lo, middle-1)
end


function _M.init(_, servers)
    local rnode_ch = {}    --element = {ip, rnode_ch_index, hash_val}
    local vnode_ch = {}    --element = {vnode_ch_index, rnode_ch_index, hash_val, ***}

    local rnode_ch_num = 4
    local vnode_ch_num = 40

    rnode_ch_num = #servers
    for i, v in pairs(servers) do
        add_rnode_ch(rnode_ch, vnode_ch, vnode_ch_num, i, v)
    end
    table.sort(vnode_ch, function(a, b) return (a[3] < b[3]) end)
    for i, v in pairs(vnode_ch) do
        v[1] = i
    end

    local self = {
        rnode_ch = rnode_ch,
        vnode_ch = vnode_ch,
        rnode_ch_num = rnode_ch_num,
        vnode_ch_num = vnode_ch_num
    }

    print("rnode_ch num: "..#self.rnode_ch)
    print("vnode_ch num: "..#self.vnode_ch)
    print("Initial Done")

    return setmetatable(self, mt)
end

function _M.request(self, id, size)
    print("id = ",id,"size=",size)
    local vnode_ch_index = hash_find_ch(self, crc32(id), 1, self.vnode_ch_num * self.rnode_ch_num)
    print(self.rnode_ch[self.vnode_ch[vnode_ch_index][2]][1])
    return self.rnode_ch[self.vnode_ch[vnode_ch_index][2]][1]   --return rnode_ch ip
end


return _M;
