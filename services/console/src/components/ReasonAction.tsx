import { Button, Form, Input, Modal, message } from "antd";
import { useRef, useState, type ReactNode } from "react";
import { describe, newIdempotencyKey } from "../api/client";

// 需要写原因的管理写操作：弹窗填原因后提交。一次弹窗内的重试沿用同一个幂等键，避免重复执行。
// fieldLabel 改写填写项的名字（如反馈案例转题时填候选题 ID）
export function ReasonAction({
  label,
  title,
  danger,
  disabled,
  icon,
  fieldLabel = "原因",
  onSubmit,
  onDone,
}: {
  label: string;
  title: string;
  danger?: boolean;
  disabled?: boolean;
  icon?: ReactNode;
  fieldLabel?: string;
  onSubmit: (reason: string, idempotencyKey: string) => Promise<string | void>;
  onDone?: () => void;
}) {
  const [open, setOpen] = useState(false);
  const [busy, setBusy] = useState(false);
  const [form] = Form.useForm<{ reason: string }>();
  const keyRef = useRef(newIdempotencyKey());
  async function submit() {
    const { reason } = await form.validateFields();
    setBusy(true);
    try {
      const note = await onSubmit(reason.trim(), keyRef.current);
      void message.success(note || `${label}：已提交`);
      setOpen(false);
      form.resetFields();
      keyRef.current = newIdempotencyKey();
      onDone?.();
    } catch (e) {
      void message.error(describe(e));
    } finally {
      setBusy(false);
    }
  }
  return (
    <>
      <Button size="small" danger={danger} disabled={disabled} icon={icon} onClick={() => setOpen(true)}>
        {label}
      </Button>
      <Modal
        open={open}
        title={title}
        okText="提交"
        cancelText="取消"
        confirmLoading={busy}
        onOk={() => void submit()}
        onCancel={() => setOpen(false)}
      >
        <Form form={form} layout="vertical">
          <Form.Item
            name="reason"
            label={fieldLabel}
            rules={[{ required: true, whitespace: true, max: 500, message: `请填写${fieldLabel}（不超过 500 字）` }]}
          >
            <Input.TextArea autoSize={{ minRows: 2 }} maxLength={500} showCount />
          </Form.Item>
        </Form>
      </Modal>
    </>
  );
}
