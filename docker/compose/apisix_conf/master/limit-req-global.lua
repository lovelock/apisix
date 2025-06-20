--
-- Licensed to the Apache Software Foundation (ASF) under one or more
-- contributor license agreements.  See the NOTICE file distributed with
-- this work for additional information regarding copyright ownership.
-- The ASF licenses this file to You under the Apache License, Version 2.0
-- (the "License"); you may not use this file except in compliance with
-- the License.  You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
--

local limit_req_new = require("resty.limit.req").new
local core = require("apisix.core")

-- 全局实例数缓存
local instance_count = 1

-- 更新实例数
local function update_instance_count()
    local core_etcd = require("apisix.core.etcd")
    local res, err = core_etcd.get("/instances", true)
    if not res or not res.body or not res.body.node or not res.body.node.nodes then
        core.log.error("failed to get instance list from etcd: ", err)
        instance_count = 1
    else
        local cnt = #res.body.node.nodes
        if cnt > 0 then
            instance_count = cnt
        else
            instance_count = 1
        end
        core.log.info("update instance_count: ", instance_count)
    end
end

-- init_worker 阶段 watch /instances（推模式）
local function watch_instances()
    local core_etcd = require("apisix.core.etcd")
    local etcd_cli, prefix, err = core_etcd.get_etcd_cli()
    if not etcd_cli then
        core.log.error("failed to get etcd cli for watch: ", err)
        return
    end
    local key = prefix .. "/instances"
    local opts = { timeout = 0 } -- 0 表示长连接
    local function handle_watch(event)
        -- event 结构见 core.etcd.watch_format
        local cnt = 1
        if event and event.body and event.body.node then
            if type(event.body.node) == "table" then
                cnt = #event.body.node
                if cnt == 0 then cnt = 1 end
            end
        end
        instance_count = cnt
        core.log.info("watchdir update instance_count: ", instance_count)
    end
    local ok, err = etcd_cli:watchdir(key, opts, handle_watch)
    if not ok then
        core.log.error("failed to start etcd watchdir: ", err)
    end
end

local plugin_name = "limit-req-global"
local sleep = core.sleep

local lrucache = core.lrucache.new({
    type = "plugin",
})

local schema = {
    type = "object",
    properties = {
        rate = {type = "number", exclusiveMinimum = 0},
        burst = {type = "number",  minimum = 0},
        key = {type = "string"},
        key_type = {
            type = "string",
            enum = {"var", "var_combination"},
            default = "var",
        },
        rejected_code = {
            type = "integer", minimum = 200, maximum = 599, default = 503
        },
        rejected_msg = {
            type = "string", minLength = 1
        },
        nodelay = {
            type = "boolean", default = false
        },
        allow_degradation = {type = "boolean", default = false}
    },
    required = {"rate", "burst", "key"},
}

local _M = {
    version = 0.1,
    priority = 1001,
    name = plugin_name,
    schema = schema,
}

function _M.init_worker()
    update_instance_count()
    watch_instances()
end

function _M.check_schema(conf)
    local ok, err = core.schema.check(schema, conf)
    if not ok then
        return false, err
    end

    return true
end

local function create_limit_obj(conf)
    core.log.info("create new limit-req-global plugin instance")
    return limit_req_new("plugin-limit-req-global", conf.rate, conf.burst)
end

function _M.access(conf, ctx)
    local real_rate = conf.rate
    if instance_count > 0 then
        real_rate = conf.rate / instance_count
        core.log.info("global limit: rate=", conf.rate, " instance_count=", instance_count, " real_rate=", real_rate)
    else
        core.log.warn("global limit: instance count is 0, fallback to single instance rate")
    end

    -- 复制 conf，避免污染原始配置
    local conf2 = core.table.clone(conf)
    conf2.rate = real_rate

    local lim, err = core.lrucache.plugin_ctx(lrucache, ctx, nil,
                                              create_limit_obj, conf2)
    if not lim then
        core.log.error("failed to instantiate a resty.limit.req object: ", err)
        if conf.allow_degradation then
            return
        end
        return 500
    end

    local conf_key = conf.key
    local key
    if conf.key_type == "var_combination" then
        local err, n_resolved
        key, err, n_resolved = core.utils.resolve_var(conf_key, ctx.var)
        if err then
            core.log.error("could not resolve vars in ", conf_key, " error: ", err)
        end

        if n_resolved == 0 then
            key = nil
        end

    else
        key = ctx.var[conf_key]
    end

    if key == nil then
        core.log.info("The value of the configured key is empty, use client IP instead")
        -- When the value of key is empty, use client IP instead
        key = ctx.var["remote_addr"]
    end

    key = key .. ctx.conf_type .. ctx.conf_version
    core.log.info("limit key: ", key)

    local delay, err = lim:incoming(key, true)
    if not delay then
        if err == "rejected" then
            if conf.rejected_msg then
                return conf.rejected_code, { error_msg = conf.rejected_msg }
            end
            return conf.rejected_code
        end

        core.log.error("failed to limit req: ", err)
        if conf.allow_degradation then
            return
        end
        return 500
    end

    if delay >= 0.001 and not conf.nodelay then
        sleep(delay)
    end
end

return _M