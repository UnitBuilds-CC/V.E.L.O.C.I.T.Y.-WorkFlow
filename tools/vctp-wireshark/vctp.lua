-- vctp.lua — Wireshark dissector for VCTP (Velocity Transfer Protocol)
--
-- Installation:
--   1. Copy this file to your Wireshark plugins directory:
--      - Windows: %APPDATA%\Wireshark\plugins\vctp.lua
--      - macOS:   ~/.local/lib/wireshark/plugins/vctp.lua
--      - Linux:   ~/.local/lib/wireshark/plugins/vctp.lua
--   2. Restart Wireshark or reload Lua plugins (Ctrl+Shift+L)
--   3. VCTP packets on UDP will be automatically decoded
--
-- Features:
--   - Parses 28-byte VCTP header: magic, sequence, workflow_id, slab_offset, payload_length
--   - Decodes JSON payload as sub-tree
--   - Validates CRC32 checksum
--   - Colorizes by method type (lifecycle=green, signal=blue, error=red)
--   - Identifies ACK packets

local vctp = Proto("vctp", "VCTP (Velocity Transfer Protocol)")

-- Constants
local VCTP_MAGIC = 0x50544356  -- "VCTP"
local VCTP_ACK_MAGIC = 0x4B435656  -- "VVCK"
local VCTP_HEADER_SIZE = 28

-- Method ID to name mapping
local method_names = {
    [100] = "StartWorkflow",
    [101] = "SignalWorkflow",
    [102] = "QueryWorkflow",
    [103] = "CancelWorkflow",
    [104] = "TerminateWorkflow",
    [105] = "DescribeWorkflow",
    [106] = "ListWorkflows",
    [107] = "ResetWorkflow",
    [108] = "UpdateWorkflow",
    [109] = "CompleteWorkflow",
    [200] = "PollWorkflowTask",
    [201] = "PollActivityTask",
    [202] = "CompleteWorkflowTask",
    [203] = "CompleteActivityTask",
    [300] = "RegisterNamespace",
    [301] = "DescribeNamespace",
    [302] = "UpdateNamespace",
    [303] = "DeleteNamespace",
    [400] = "GetHistory",
    [401] = "GetWorkflowExecution",
    [500] = "HealthCheck",
    [501] = "RecordHeartbeat",
    [502] = "CountWorkflows",
    [503] = "BatchSignal",
    [504] = "BatchTerminate",
    [600] = "StartChildWorkflow",
    [601] = "ContinueAsNew",
    [602] = "ScheduleTimer",
    [603] = "CancelTimer",
    [604] = "SetMemo",
    [605] = "UpsertSearchAttributes",
    [606] = "SignalWithStart",
}

-- Method category for colorization
local function get_method_category(method_id)
    if method_id == nil then return "unknown" end
    if method_id >= 100 and method_id <= 109 then return "lifecycle" end
    if method_id >= 200 and method_id <= 203 then return "task" end
    if method_id >= 300 and method_id <= 303 then return "namespace" end
    if method_id >= 400 and method_id <= 401 then return "history" end
    if method_id >= 500 and method_id <= 504 then return "system" end
    if method_id >= 600 and method_id <= 606 then return "advanced" end
    return "unknown"
end

-- Header fields
local f_magic    = ProtoField.uint32("vctp.magic", "Magic", base.HEX, nil, nil, "VCTP magic bytes")
local f_sequence = ProtoField.uint64("vctp.sequence", "Sequence Number", base.DEC, nil, nil, "Monotonic packet sequence ID")
local f_workflow = ProtoField.uint64("vctp.workflow_id", "Workflow/Method ID", base.DEC, nil, nil, "Workflow ID or method identifier")
local f_method   = ProtoField.string("vctp.method", "Method Name", nil, "Resolved method name")
local f_slab     = ProtoField.uint32("vctp.slab_offset", "Slab Offset / Fragment Meta", base.HEX, nil, nil, "Slab offset or fragment metadata")
local f_frag_idx = ProtoField.uint16("vctp.fragment_index", "Fragment Index", base.DEC, nil, nil, "Fragment index (from slab_offset)")
local f_frag_tot = ProtoField.uint16("vctp.fragment_total", "Fragment Total", base.DEC, nil, nil, "Total fragments (from slab_offset)")
local f_paylen   = ProtoField.uint32("vctp.payload_length", "Payload Length", base.DEC, nil, nil, "Length of payload in bytes")
local f_payload  = ProtoField.bytes("vctp.payload", "Payload", nil, "Raw payload bytes")
local f_json     = ProtoField.string("vctp.json", "JSON Payload", nil, "Decoded JSON payload")
local f_crc32    = ProtoField.uint32("vctp.checksum", "CRC32 Checksum", base.HEX, nil, nil, "Packet integrity checksum")
local f_crc_ok   = ProtoField.bool("vctp.checksum_valid", "Checksum Valid", nil, "Whether CRC32 matches")

-- ACK fields
local f_ack_magic    = ProtoField.uint32("vctp.ack_magic", "ACK Magic", base.HEX, nil, nil, "VCTP ACK magic bytes")
local f_ack_sequence = ProtoField.uint64("vctp.ack_sequence", "ACK Sequence", base.DEC, nil, nil, "Sequence number being acknowledged")
local f_ack_workflow = ProtoField.uint64("vctp.ack_workflow_id", "ACK Workflow ID", base.DEC, nil, nil, "Workflow ID of acknowledged packet")

vctp.fields = {
    f_magic, f_sequence, f_workflow, f_method, f_slab,
    f_frag_idx, f_frag_tot, f_paylen, f_payload, f_json,
    f_crc32, f_crc_ok,
    f_ack_magic, f_ack_sequence, f_ack_workflow,
}

-- CRC32 computation (matching zlib.crc32)
local function compute_crc32(buffer)
    -- Use Wireshark's built-in CRC check
    return buffer:crc32()
end

-- Main dissector function
function vctp.dissector(buffer, pinfo, tree)
    local length = buffer:len()
    if length < 20 then return false end  -- Too small for even an ACK

    local magic = buffer(0, 4):uint()

    -- Check for ACK packet
    if magic == VCTP_ACK_MAGIC then
        if length < 20 then return false end

        pinfo.cols.protocol = "VCTP-ACK"
        pinfo.cols.info = "VCTP ACK"

        local subtree = tree:add(vctp, buffer(), "VCTP ACK Packet")
        subtree:add(f_ack_magic, buffer(0, 4)):append_text(" (ACK)")
        subtree:add(f_ack_sequence, buffer(4, 8))
        subtree:add(f_ack_workflow, buffer(12, 8))
        return true
    end

    -- Check for VCTP packet
    if magic ~= VCTP_MAGIC then return false end

    if length < VCTP_HEADER_SIZE then
        pinfo.cols.protocol = "VCTP"
        pinfo.cols.info = "VCTP (truncated header)"
        local subtree = tree:add(vctp, buffer(), "VCTP Packet (truncated)")
        subtree:add(f_magic, buffer(0, 4)):append_text(" (VCTP)")
        return true
    end

    -- Parse header fields
    local sequence = buffer(4, 8):uint64()
    local workflow_id = buffer(12, 8):uint64()
    local method_id = tonumber(tostring(workflow_id))
    local slab_offset = buffer(20, 4):uint()
    local payload_length = buffer(24, 4):uint()

    -- Resolve method name
    local method_name = method_names[method_id] or string.format("Unknown(%d)", method_id)
    local category = get_method_category(method_id)

    -- Set protocol columns
    pinfo.cols.protocol = "VCTP"
    pinfo.cols.info = string.format("VCTP %s (seq=%s)", method_name, tostring(sequence))

    -- Build info column with more detail for request/response
    if payload_length > 0 and length >= VCTP_HEADER_SIZE + payload_length then
        local payload_buf = buffer(VCTP_HEADER_SIZE, payload_length)
        local payload_str = payload_buf:string()
        -- Try to extract method/status from JSON
        local status_match = payload_str:match('"status"%s*:%s*(%d+)')
        if status_match and status_match ~= "0" then
            pinfo.cols.info = string.format("VCTP %s ERROR %s (seq=%s)", method_name, status_match, tostring(sequence))
        end
    end

    -- Create protocol tree
    local subtree = tree:add(vctp, buffer(), string.format("VCTP Protocol — %s", method_name))

    -- Header fields
    subtree:add(f_magic, buffer(0, 4)):append_text(" (VCTP)")
    subtree:add(f_sequence, buffer(4, 8))
    subtree:add(f_workflow, buffer(12, 8))
    subtree:add(f_method, method_name)

    -- Fragment metadata (if slab_offset encodes fragments)
    local frag_index = bit.rshift(slab_offset, 16)
    local frag_total = bit.band(slab_offset, 0xFFFF)
    if frag_total > 1 then
        subtree:add(f_slab, buffer(20, 4)):append_text(string.format(" (Fragment %d/%d)", frag_index, frag_total))
        subtree:add(f_frag_idx, frag_index)
        subtree:add(f_frag_tot, frag_total)
    else
        subtree:add(f_slab, buffer(20, 4))
    end

    subtree:add(f_paylen, buffer(24, 4))

    -- Payload
    if payload_length > 0 and length >= VCTP_HEADER_SIZE + payload_length + 4 then
        local payload_buf = buffer(VCTP_HEADER_SIZE, payload_length)
        local payload_str = payload_buf:string()

        -- Try JSON decode
        local json_item = subtree:add(f_json, payload_str)
        -- Pretty-print JSON in the details
        local pretty = payload_str:gsub(',"', ',\n  "'):gsub('{', '{\n  '):gsub('}', '\n}')
        json_item:set_text("JSON Payload: " .. pretty:sub(1, 80) .. "...")

        -- Also add raw bytes
        subtree:add(f_payload, payload_buf)

        -- CRC32 verification
        local packet_data = buffer(0, VCTP_HEADER_SIZE + payload_length)
        local expected_crc = buffer(VCTP_HEADER_SIZE + payload_length, 4):uint()
        local actual_crc = compute_crc32(packet_data)
        local crc_valid = (expected_crc == actual_crc)

        local crc_item = subtree:add(f_crc32, buffer(VCTP_HEADER_SIZE + payload_length, 4))
        if crc_valid then
            crc_item:append_text(" (valid)")
        else
            crc_item:append_text(string.format(" (INVALID! expected=0x%08X)", actual_crc))
            crc_item:set_generated()
        end
        subtree:add(f_crc_ok, crc_valid)

        -- Colorize by category
        if category == "lifecycle" then
            pinfo.cols.info:append_text(" [LIFECYCLE]")
        elseif category == "signal" then
            pinfo.cols.info:append_text(" [SIGNAL]")
        elseif category == "system" then
            pinfo.cols.info:append_text(" [SYSTEM]")
        end
    elseif payload_length == 0 then
        -- No payload, check for CRC
        if length >= VCTP_HEADER_SIZE + 4 then
            local packet_data = buffer(0, VCTP_HEADER_SIZE)
            local expected_crc = buffer(VCTP_HEADER_SIZE, 4):uint()
            local actual_crc = compute_crc32(packet_data)
            subtree:add(f_crc32, buffer(VCTP_HEADER_SIZE, 4))
            subtree:add(f_crc_ok, expected_crc == actual_crc)
        end
    end

    return true
end

-- Register VCTP dissector for UDP
-- VCTP uses dynamic ports, so we register as a heuristic dissector
local function heuristic_dissector(buffer, pinfo, tree)
    if buffer:len() < 4 then return false end
    local magic = buffer(0, 4):uint()
    if magic == VCTP_MAGIC or magic == VCTP_ACK_MAGIC then
        vctp.dissector(buffer, pinfo, tree)
        return true
    end
    return false
end

vctp:register_heuristic(heuristic_dissector)

-- Also allow manual decode via "Decode As..." for specific UDP ports
local udp_table = DissectorTable.get("udp.port")
-- Common VCTP ports (can be overridden in Wireshark's Decode As)
udp_table:add(9090, vctp)
udp_table:add(9091, vctp)
