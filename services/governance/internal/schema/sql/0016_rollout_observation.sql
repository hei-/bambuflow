-- 灰度观察（governance 详设 4.1 第 ⑨ 步）：evaluation 定时比较候选组与对照组，最近一次的结果（两组样本数与各项指标）记在灰度上，
-- console 灰度列表展示；超阈值时终止灰度，观察期满且没有超阈值时全量。
ALTER TABLE governance.rollouts
    ADD COLUMN observation text NOT NULL DEFAULT '',
    ADD COLUMN observed_at timestamptz;
