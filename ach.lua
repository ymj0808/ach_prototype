-- adaptive consistent hash --

local _M = {}                 -- module of consistent
local mt = { __index = _M }


local crc32 = function(arg)            -- crc32 algorithm
    return math.abs(ngx.crc32_long(arg))
    --return math.abs(tonumber(arg) * 1234321 % 999)
end

function table_copy(src, dest)
  for k, v in pairs(src) do
    if type(v) == "table" then
      dest[k] = {}
      table_copy(v, dest[k])
    else
      dest[k] =  v
    end
  end
end

function table_len(t)
  local len=0
  for k, v in pairs(t) do
    len=len+1
  end
  return len;
end

function table_remove(tbl,key)
    local tmp ={}
    for i in pairs(tbl) do
        table.insert(tmp,i)
    end
    local newTbl = {}
    local i = 1
    while i <= table_len(tmp) do
        local val = tmp [i]
        if val == key then
            table.remove(tmp,i)
        else
            newTbl[val] = tbl[val]
            i = i + 1
         end
    end
    return newTbl
end


function dequeue_init()
    local tmp_last_access = 1
    local tmp_vnode_index = 1
    local tmp_size_vnode = 0
    local tmp_rnode_index = 1
    local tmp_size_rnode = 0
    local tmp_content_size = 0
    local tmp_c_rnode = 0
    local tmp_c_vnode = 0
    local tmp_frag_arrs_vnode = {}
    local tmp_frag_arrs_vnode_key = {}
    local tmp_frag_arrs_rnode = {}
    local tmp_frag_arrs_rnode_key = {}

    local dest = {tmp_last_access, {tmp_vnode_index, tmp_rnode_index, tmp_content_size, tmp_size_vnode, tmp_size_rnode,
                              tmp_c_vnode, tmp_c_rnode, tmp_frag_arrs_vnode_key, tmp_frag_arrs_vnode,
                              tmp_frag_arrs_rnode_key, tmp_frag_arrs_rnode}}
    return dest
end

function copy_arr_vnode(frag_arr_vnode, frag_arr_vnode_key, vnode_index, rnode_index, size, dest)
    dest[2][1] = vnode_index
    dest[2][2] = rnode_index
    dest[2][3] = size
    dest[2][4] = table_len(frag_arr_vnode)
    table_copy(frag_arr_vnode, dest[2][9])
    table_copy(frag_arr_vnode_key, dest[2][8])
end

function copy_arr_rnode(frag_arr_rnode, frag_arr_rnode_key, dest)
    dest[2][5] = table_len(frag_arr_rnode)
    table_copy(frag_arr_rnode_key, dest[2][10])
    table_copy(frag_arr_rnode, dest[2][11])
end

function c_value_rnode(pos, deq)
    local arr_rnode = deq[2][10]
    local tmp_pos = deq[2][5]
    --print('tmp_pos: '..tmp_pos, '#arr_rnode：'..table_len(arr_rnode))
    if(pos > arr_rnode[tmp_pos]) then
        deq[2][6] = 0
        return 0
    end
    local lo = 1
    local high = deq[2][5] + 1
    local mid = 1
    while(lo + 1 < high) do
        mid = math.floor((lo + high) / 2)
        --print('mid='..arr_rnode[mid])
        if pos < arr_rnode[mid] then
            high = mid
        elseif pos > arr_rnode[mid] then
            lo = mid
        else
            lo = mid
            break
        end
    end
    deq[2][6] = deq[2][11][arr_rnode[lo]]
    return deq[2][6]
end


--element = {last_access,
-- {vnode_index, rnode_index, content_size, size_vnode, size_rnode, c_rnode, c_vnode, vnode_arr_key, vnode_arr, rnode_arr_key, rnode_arr}}
--  [2][1]        [2][2]        [2][3]        [2][4]      [2][5]     [2][6]    [2][7]     [2][8]      [2][9]     [2][10]       [2][11]

function c_value_vnode(pos, deq)
    local arr_vnode = deq[2][8]
    local tmp_pos = deq[2][4]
    if(pos > arr_vnode[tmp_pos]) then
        return 0
    end
    local lo = 1
    local high = deq[2][4] + 1
    local mid = 1
    while(lo + 1 < high) do
        mid = math.floor((lo + high) / 2)
        if pos < arr_vnode[mid] then
            high = mid
        elseif pos > arr_vnode[mid] then
            lo = mid
        else
            lo = mid
            break
        end
    end
    deq[2][7] = deq[2][9][arr_vnode[lo]]
    return deq[2][7]
end

function add_rnode(rnode, vnode, vnode_num, index, ip)
    local tmp_rnode_index = index
    local tmp_rnode_ip = ip
    local tmp_rnode_hash_val = crc32(tmp_rnode_ip)
    table.insert(rnode, {ip, tmp_rnode_index, tmp_rnode_hash_val})

    for i = 1, vnode_num do
        --local tmp_vnode_ip = tmp_rnode_ip..'-'..i
        local tmp_vnode_ip = tmp_rnode_ip..i
        local tmp_vnode_hash_val = crc32(tmp_vnode_ip)
        local tmp_vnode_index = i
        table.insert(vnode, {tmp_vnode_index, tmp_rnode_index, tmp_vnode_hash_val})
    end
end

-- vnode--element = {vnode_index, rnode_index, hash_val, ***}
function hash_find(self, key, lo, hi)
    print("ach hashfind")
    if key <= self.vnode[lo][3] or key > self.vnode[hi][3] then
        return self.vnode[lo][1]
    end

    local middle = lo + math.floor((hi - lo) / 2)
    if middle == 1 then
        return self.vnode[middle][1]
    elseif key <=self.vnode[middle][3] and key > self.vnode[middle-1][3] then
        return self.vnode[middle][1]
    elseif key > self.vnode[middle][3] then
        return hash_find(self, key, middle+1, hi)
    end
    return hash_find(self, key, lo, middle-1)
end


function _M.init(_, servers)
    local threshold = 300000000   -- cache size
    local window = 10          --update window

    local rnode = {}    --element = {ip, rnode_index, hash_val}
    local vnode = {}    --element = {vnode_index, rnode_index, hash_val, ***}

    local rnode_num = 4
    local vnode_num = 40
    local usage = {}                   -- usage of rnode
    local vnode_index_for_each_rnode = {}   -- {}
    local frag_arrs_vnode = {}
    local frag_arrs_vnode_key = {}
    local frag_arrs_rnode = {}
    local frag_arrs_rnode_key = {}
    local dequeue = {}          --element = {last_access,
                                --{vnode_index, rnode_index, size, vnode_arr_key, vnode_arr, rnode_arr_key, rnode_arr_key}}
    local last_access_on_each_rnode = {}
    local last_access_on_each_vnode = {}
    local queue_of_min = {}
    local queue_of_max = {}
    local queue_of_c_i = {}
    local position = 1

    rnode_num = #servers
    for i, v in pairs(servers) do
        add_rnode(rnode, vnode, vnode_num, i, v)
        vnode_index_for_each_rnode[i] = {}
        last_access_on_each_rnode[i] = {}
        table.insert(frag_arrs_rnode_key, {})
        table.insert(frag_arrs_rnode, {})
        table.insert(frag_arrs_rnode_key[i], 1)
        frag_arrs_rnode[i][1] = 0
        usage[i] = 0
    end

    table.sort(vnode, function(a, b) return (a[3] < b[3]) end)

    for i, v in pairs(vnode) do
        v[1] = i
        table.insert(vnode_index_for_each_rnode[v[2]], i)
        last_access_on_each_vnode[i] = {}
        table.insert(frag_arrs_vnode_key, {})
        table.insert(frag_arrs_vnode, {})
        table.insert(frag_arrs_vnode_key[i], 1)
        frag_arrs_vnode[i][1] = 0
    end

    for i = 1, window do
        dequeue[i] = dequeue_init()
    end

    local self = {
        threshold = threshold,
        window = window,

        rnode = rnode,
        vnode = vnode,

        rnode_num = rnode_num,
        vnode_num = vnode_num,
        usage = usage,
        vnode_index_for_each_rnode = vnode_index_for_each_rnode,
        frag_arrs_vnode = frag_arrs_vnode,
        frag_arrs_vnode_key = frag_arrs_vnode_key,
        frag_arrs_rnode = frag_arrs_rnode,
        frag_arrs_rnode_key = frag_arrs_rnode_key,
        dequeue = dequeue,
        last_access_on_each_rnode = last_access_on_each_rnode,
        last_access_on_each_vnode = last_access_on_each_vnode,
        queue_of_min = queue_of_min,
        queue_of_max = queue_of_max,
        queue_of_c_i = queue_of_c_i,
        position = position
    }

    return setmetatable(self, mt)
end

function _M.request(self, id, size)
    local vnode_index = hash_find(self, crc32(id), 1, self.vnode_num *  self.rnode_num)

    local iter_in_last_access = self.last_access_on_each_vnode[vnode_index][id]
    --print("last access: ", iter_in_last_access)
    if iter_in_last_access then
        local start = 1
        local tmp_pos = self.last_access_on_each_vnode[vnode_index][id]
        for i, v in pairs(self.frag_arrs_vnode_key[vnode_index]) do
            if v == tmp_pos then
                start = i
            end
        end
        local next = start+1
        start = start+2
        while(start <= table_len(self.frag_arrs_vnode_key[vnode_index])) do
            local key = self.frag_arrs_vnode_key[vnode_index][start]
            self.frag_arrs_vnode[vnode_index][key] = self.frag_arrs_vnode[vnode_index][key] + size
            start = start + 1
        end
        self.frag_arrs_vnode[vnode_index] = table_remove(self.frag_arrs_vnode[vnode_index], self.frag_arrs_vnode_key[vnode_index][next])
        table.remove(self.frag_arrs_vnode_key[vnode_index], next)
        self.dequeue[self.position][1] = self.last_access_on_each_vnode[vnode_index][id]
        self.last_access_on_each_vnode[vnode_index][id] = self.position


    else
        for i, v in pairs(self.frag_arrs_vnode_key[vnode_index]) do
            local key = v
            self.frag_arrs_vnode[vnode_index][key] = self.frag_arrs_vnode[vnode_index][key] + size
        end
        self.last_access_on_each_vnode[vnode_index][id] = self.position
        self.dequeue[self.position][1] = 0xffffffff
    end

    table.insert(self.frag_arrs_vnode_key[vnode_index], self.position+1)
    self.frag_arrs_vnode[vnode_index][self.position+1] = 0

    local rnode_index = self.vnode[vnode_index][2]
    copy_arr_vnode(self.frag_arrs_vnode[vnode_index], self.frag_arrs_vnode_key[vnode_index], vnode_index, rnode_index, size, self.dequeue[self.position])
    copy_arr_rnode(self.frag_arrs_rnode[rnode_index],self.frag_arrs_rnode_key[rnode_index], self.dequeue[self.position])
    print(self.rnode[self.vnode[vnode_index][2]][1])
    self.position = self.position + 1
    if(self.position > self.window) then
        update(self)
    end
    self.usage[self.vnode[vnode_index][2]] = self.usage[self.vnode[vnode_index][2]] + 1
    return self.rnode[self.vnode[vnode_index][2]][1]   --return rnode ip
end


function update(self)
    local max_usage = 0
    local min_usage = 0xffffffff
    local max_srv = 1
    local min_srv = 1
    for i, v in pairs(self.usage) do
        if v < min_usage then
            min_usage = v
            min_srv = i
        end
        if v > max_usage then
            max_usage = v
            max_srv = i
        end
    end

    local sd_max = 0
    self.position = 1
    for i = 1, self.window do
        local rnode_index = self.dequeue[i][2][2]
        if rnode_index == max_srv or rnode_index == min_srv then
            local size = self.dequeue[i][2][3]
            local tmp_pos = self.dequeue[i][1]
            local start = 1
            if(tmp_pos ~= 0xfffffffff) then
                for j, val in pairs(self.frag_arrs_rnode_key[rnode_index]) do
                    if val == tmp_pos then
                        start = j
                    end
                end
                local next = start + 1
                start = start + 2
                while(start <= table_len(self.frag_arrs_rnode_key[rnode_index])) do
                    local key = self.frag_arrs_rnode_key[rnode_index][start]
                    self.frag_arrs_rnode[rnode_index][key] = self.frag_arrs_rnode[rnode_index][key] + size
                    start = start + 1
                end
                self.frag_arrs_rnode[rnode_index] = table_remove(self.frag_arrs_rnode[rnode_index], self.frag_arrs_rnode_key[rnode_index][next])
                table.remove(self.frag_arrs_rnode_key[rnode_index], next)

            else
                for j, val in pairs(self.frag_arrs_rnode_key[rnode_index]) do
                    local key = val
                    self.frag_arrs_rnode[rnode_index][key] = self.frag_arrs_rnode[rnode_index][key] + size
                end
            end
            table.insert(self.frag_arrs_rnode_key[rnode_index], self.position+1)
            self.frag_arrs_rnode[rnode_index][self.position+1] = 0

            --print("arr#", #frag_arrs_rnode[rnode_index], "key#", #frag_arrs_rnode_key[rnode_index])
            copy_arr_rnode(self.frag_arrs_rnode[rnode_index], self.frag_arrs_rnode_key[rnode_index],self.dequeue[i])
            if self.dequeue[i][1] ~= 0xffffffff then
                local pos = self.dequeue[i][1]
                local sd_rnode = c_value_rnode(pos, self.dequeue[i])
                local sd_vnode = c_value_vnode(pos, self.dequeue[i])
                if(sd_rnode < self.threshold) then
                    sd_max = sd_max + 1
                end
            end
        end
        self.position = self.position + 1
    end

    --element = {last_access,
-- {vnode_index, rnode_index, content_size, size_vnode, size_rnode, c_rnode, c_vnode, vnode_arr_key, vnode_arr, rnode_arr_key, rnode_arr}}
--  [2][1]        [2][2]        [2][3]        [2][4]      [2][5]     [2][6]    [2][7]     [2][8]      [2][9]     [2][10]       [2][11]

    print('before sd_max = '..sd_max)

    local pointer = self.window
    local target = self.vnode_index_for_each_rnode[max_srv][table_len(self.vnode_index_for_each_rnode[max_srv])] + 1
    for i, v in pairs(self.vnode_index_for_each_rnode[max_srv]) do
        local sd = 0
        while(pointer > 0) do
            if self.dequeue[pointer][2][2] == min_srv then
                if self.dequeue[pointer][1] ~= 0xffffffff then
                    table.insert(self.queue_of_min, pointer)
                end
                for j, val in pairs(self.queue_of_c_i) do
                    if self.dequeue[val][2][7] + c_value_vnode(self.dequeue[val][1], self.dequeue[pointer]) < self.threshold then
                        sd = sd + 1
                    end
                end
                self.queue_of_c_i = {}
            elseif self.dequeue[pointer][2][1] == v then
                if self.dequeue[pointer][1] ~= 0xffffffff then
                    table.insert(self.queue_of_c_i,pointer)
                end
                for j, val in pairs(self.queue_of_max) do
                    if self.dequeue[val][2][7] - c_value_vnode(self.dequeue[val][1], self.dequeue[pointer]) < self.threshold then
                        sd = sd + 1
                    end
                end
                self.queue_of_max = {}
                for j, val in pairs(self.queue_of_min) do
                    if self.dequeue[val][2][7] + c_value_vnode(self.dequeue[val][1], self.dequeue[pointer]) < self.threshold then
                        sd = sd + 1
                    end
                end
                self.queue_of_min = {}
            elseif self.dequeue[pointer][2][2] == max_srv and self.dequeue[pointer][1] ~= 0xffffffff then
                table.insert(self.queue_of_max, pointer)
            end
            pointer = pointer - 1
        end

        for j, val in pairs(self.queue_of_min) do
            if self.dequeue[val][2][7] < self.threshold then
                sd = sd + 1
            end
        end
        self.queue_of_min = {}

        for j, val in pairs(self.queue_of_max) do
            if self.dequeue[val][2][7] < self.threshold then
                sd = sd + 1
            end
        end
        self.queue_of_max = {}

        for j, val in pairs(self.queue_of_c_i) do
            if self.dequeue[val][2][7] < self.threshold then
                sd = sd + 1
            end
        end
        self.queue_of_c_i = {}

        if sd > sd_max then
            sd_max = sd
            target = v
        end

        pointer = self.window
    end

    print('after sd_max = '..sd_max)

    if target ~= self.vnode_index_for_each_rnode[max_srv][table_len(self.vnode_index_for_each_rnode[max_srv])] + 1 then
        self.vnode[target][2] = min_srv
        table.insert(self.vnode_index_for_each_rnode[min_srv], target)
        table.erase(self.vnode_index_for_each_rnode[max_srv], target)
    end
    reset(self)
end

function reset(self)
    print("reset...")
    self.position = 1

    for i, v in pairs(self.dequeue) do
        v = dequeue_init()
    end

    self.frag_arrs_rnode_key = {}
    self.frag_arrs_rnode = {}
    self.frag_arrs_vnode_key = {}
    self.frag_arrs_vnode = {}
    self.last_access_on_each_rnode ={}

    for i = 1, self.rnode_num do
        self.last_access_on_each_rnode[i] = {}
        table.insert(self.frag_arrs_rnode_key, {})
        table.insert(self.frag_arrs_rnode, {})
        table.insert(self.frag_arrs_rnode_key[i], 1)
        self.frag_arrs_rnode[i][1] = 0
        self.usage[i] = 0
    end

    for i, v in pairs(self.vnode) do
        self.last_access_on_each_vnode[i] = {}
        table.insert(self.frag_arrs_vnode_key, {})
        table.insert(self.frag_arrs_vnode, {})
        table.insert(self.frag_arrs_vnode_key[i], 1)
        self.frag_arrs_vnode[i][1] = 0
    end
end

return _M;
