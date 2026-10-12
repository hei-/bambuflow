-- console 修改 Git 类配置对象（pipeline、routing、policy、prompt）时，变更单不在这里审批生效，而是写成合并请求
-- （governance 详设 3.1）：状态记为 merge_request，merge_request_url 是合并请求的链接。合并后由 CI 调发布接口生效。
ALTER TABLE governance.change_requests DROP CONSTRAINT change_requests_status_check;
ALTER TABLE governance.change_requests
    ADD CONSTRAINT change_requests_status_check CHECK (status IN ('open', 'approved', 'rejected', 'stale', 'merge_request')),
    ADD COLUMN merge_request_url text NOT NULL DEFAULT '';
