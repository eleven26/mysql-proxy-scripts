--
-- User: ruby
-- Date: 2019/1/21
-- Time: 1:16 PM
--

-- 1. 输出执行时间
-- 2. 输出可执行的 prepare sql 语句, 使用了实际参数值代替了语句中的 '?'

local proto = require("mysql.proto")

local prep_stmts = {}

local start = os.clock();

function read_query(packet)
    local cmd_type = packet:byte()
    if cmd_type == proxy.COM_STMT_PREPARE then
        proxy.queries:append(1, packet, { resultset_is_needed = true })
        return proxy.PROXY_SEND_QUERY
    elseif cmd_type == proxy.COM_STMT_EXECUTE then
        proxy.queries:append(2, packet, { resultset_is_needed = true })
        return proxy.PROXY_SEND_QUERY
    elseif cmd_type == proxy.COM_STMT_CLOSE then
        proxy.queries:append(3, packet, { resultset_is_needed = true })
        return proxy.PROXY_SEND_QUERY
    end
end

function read_query_result(inj)
    if inj.id == 1 then
        -- print the query we sent
        local stmt_prepare = assert(proto.from_stmt_prepare_packet(inj.query))
        -- print(("> PREPARE: %s"):format(stmt_prepare.stmt_text))

        -- and the stmt-id we got for it
        if inj.resultset.raw:byte() == 0 then
            local stmt_prepare_ok = assert(proto.from_stmt_prepare_ok_packet(inj.resultset.raw))
            -- print(("< PREPARE: stmt-id = %d (resultset-cols = %d, params = %d)"):format(
            -- 	stmt_prepare_ok.stmt_id,
            -- 	stmt_prepare_ok.num_columns,
            -- 	stmt_prepare_ok.num_params))

            prep_stmts[stmt_prepare_ok.stmt_id] = {
                stmt_text = stmt_prepare.stmt_text,
                num_columns = stmt_prepare_ok.num_columns,
                num_params = stmt_prepare_ok.num_params,
            }
        end
    elseif inj.id == 2 then
        local stmt_id = assert(proto.stmt_id_from_stmt_execute_packet(inj.query))
        local stmt_execute = assert(proto.from_stmt_execute_packet(inj.query, prep_stmts[stmt_id].num_params))
        -- print(("> EXECUTE: stmt-id = %d"):format(stmt_execute.stmt_id))
        local sql = prep_stmts[stmt_id].stmt_text

        if stmt_execute.new_params_bound then
            for ndx, v in ipairs(stmt_execute.params) do
                -- replace '?' to truly value of prepare statements.
                if (v.type == proxy.MYSQL_TYPE_LONGLONG) then
                    sql = string.gsub(sql, '?', v.value, 1)
                else
                    sql = string.gsub(sql, '?', '"' .. v.value .. '"', 1)
                end
            end
        end

        if string.sub(sql, 0, 3) ~= 'set' then
            print("time consume: " .. (os.clock() - start) * 1000 .. " ms")
            print(sql)
        end

    elseif inj.id == 3 then
        local stmt_close = assert(proto.from_stmt_close_packet(inj.query))
        print(("> CLOSE: stmt-id = %d"):format(stmt_close.stmt_id))

        prep_stmts[stmt_close.stmt_id] = nil -- cleanup
    end
end

