import { Button, Popconfirm, message } from "antd";
import { useRef, useState } from "react";
import { describe, newIdempotencyKey } from "../api/client";

// 不用填原因的管理写操作（如审批）：确认后提交。失败后再点沿用同一个幂等键，成功后换新键
export function ConfirmAction({
  label,
  title,
  danger,
  disabled,
  onSubmit,
  onDone,
}: {
  label: string;
  title: string;
  danger?: boolean;
  disabled?: boolean;
  onSubmit: (idempotencyKey: string) => Promise<string | void>;
  onDone?: () => void;
}) {
  const [busy, setBusy] = useState(false);
  const keyRef = useRef(newIdempotencyKey());
  async function submit() {
    setBusy(true);
    try {
      const note = await onSubmit(keyRef.current);
      keyRef.current = newIdempotencyKey();
      void message.success(note || `${label}：已提交`);
      onDone?.();
    } catch (e) {
      void message.error(describe(e));
    } finally {
      setBusy(false);
    }
  }
  return (
    <Popconfirm title={title} okText="确定" cancelText="取消" onConfirm={() => void submit()} disabled={disabled}>
      <Button size="small" type="primary" danger={danger} loading={busy} disabled={disabled}>
        {label}
      </Button>
    </Popconfirm>
  );
}
