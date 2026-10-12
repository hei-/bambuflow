import { Alert, Button } from "antd";
import { ApiError, describe } from "../api/client";

// 错误提示：可重试的错误给出“重试”；带追踪 ID 的显示出来，便于排查
export function ErrorAlert({ error, onRetry }: { error: unknown; onRetry?: () => void }) {
  const traceId = error instanceof ApiError ? error.traceId : undefined;
  return (
    <Alert
      type="error"
      showIcon
      message={describe(error)}
      description={traceId ? `追踪 ID：${traceId}` : undefined}
      action={
        onRetry ? (
          <Button size="small" onClick={onRetry}>
            重试
          </Button>
        ) : undefined
      }
    />
  );
}
