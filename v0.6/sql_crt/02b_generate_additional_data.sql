-- =====================================================
-- AML300数据库补充数据生成脚本
-- 包含账户、交易、风险评估等核心业务数据
-- 创建时间: 2025-11-11
-- 版本: v1.0
-- =====================================================

USE AML300;

-- =====================================================
-- 4. 账户数据生成 (约1200个账户)
-- =====================================================
-- 为个人客户生成账户
INSERT INTO tb_acc (Self_acc_no, Cst_no, Acc_type, Acc_name, Acc_cur, Acc_state, Open_time, Open_bank)
SELECT
    CONCAT('ACC', LPAD(p.seq + (u.seq * 1000), 12, '0')) as Self_acc_no,
    p.Cst_no,
    '11' as Acc_type,
    p.Acc_name as Acc_name,
    'CNY' as Acc_cur,
    CASE WHEN RAND() > 0.05 THEN '1' ELSE '3' END as Acc_state,
    CONCAT(2010 + FLOOR(RAND() * 15), LPAD(1 + FLOOR(RAND() * 12), 2, '0'), LPAD(1 + FLOOR(RAND() * 28), 2, '0')) as Open_time,
    CASE FLOOR(RAND() * 20)
        WHEN 0 THEN '104100000019'
        WHEN 1 THEN '104100000020'
        WHEN 2 THEN '104100000021'
        WHEN 3 THEN '104100000022'
        WHEN 4 THEN '104310000001'
        WHEN 5 THEN '104310000002'
        WHEN 6 THEN '104310000003'
        WHEN 7 THEN '104440300001'
        WHEN 8 THEN '104440300002'
        WHEN 9 THEN '104440300003'
        WHEN 10 THEN '104330100001'
        WHEN 11 THEN '104330100002'
        WHEN 12 THEN '104510100001'
        WHEN 13 THEN '104510100002'
        WHEN 14 THEN '104420100001'
        WHEN 15 THEN '104420100002'
        WHEN 16 THEN '104320100001'
        WHEN 17 THEN '104320100002'
        WHEN 18 THEN '104120100001'
        ELSE '104120100002'
    END as Open_bank
FROM tb_cst_pers p
CROSS JOIN (SELECT 1 as seq UNION SELECT 2 UNION SELECT 3) u
WHERE p.seq <= 400 -- 为前400个个人客户生成账户
LIMIT 1200;

-- 为企业客户生成账户
INSERT INTO tb_acc (Self_acc_no, Cst_no, Acc_type, Acc_name, Acc_cur, Acc_state, Open_time, Open_bank)
SELECT
    CONCAT('BACC', LPAD(seq, 10, '0')) as Self_acc_no,
    Cst_no,
    '13' as Acc_type,
    Acc_name as Acc_name,
    'CNY' as Acc_cur,
    CASE WHEN RAND() > 0.02 THEN '1' ELSE '3' END as Acc_state,
    CONCAT(2010 + FLOOR(RAND() * 15), LPAD(1 + FLOOR(RAND() * 12), 2, '0'), LPAD(1 + FLOOR(RAND() * 28), 2, '0')) as Open_time,
    CASE FLOOR(RAND() * 20)
        WHEN 0 THEN '104100000019'
        WHEN 1 THEN '104100000020'
        WHEN 2 THEN '104100000021'
        WHEN 3 THEN '104100000022'
        WHEN 4 THEN '104310000001'
        WHEN 5 THEN '104310000002'
        WHEN 6 THEN '104310000003'
        WHEN 7 THEN '104440300001'
        WHEN 8 THEN '104440300002'
        WHEN 9 THEN '104440300003'
        WHEN 10 THEN '104330100001'
        WHEN 11 THEN '104330100002'
        WHEN 12 THEN '104510100001'
        WHEN 13 THEN '104510100002'
        WHEN 14 THEN '104420100001'
        WHEN 15 THEN '104420100002'
        WHEN 16 THEN '104320100001'
        WHEN 17 THEN '104320100002'
        WHEN 18 THEN '104120100001'
        ELSE '104120100002'
    END as Open_bank
FROM tb_cst_unit
WHERE CAST(SUBSTRING(Cst_no, 2) AS UNSIGNED) <= 100 -- 所有企业客户都生成账户
LIMIT 100;

-- =====================================================
-- 5. 风险等级评估数据生成 (110个高风险客户)
-- =====================================================
-- 高风险个人客户 (100个)
INSERT INTO tb_risk_new (Cst_no, Acc_type, Risk_code, Risk_score, Time, Risk_reason, Evaluator)
SELECT
    Cst_no,
    '11' as Acc_type,
    '01' as Risk_code,
    80 + FLOOR(RAND() * 20) as Risk_score,
    CONCAT(
        '2020',
        LPAD(FLOOR(RAND() * 6) + 1, 2, '0'),
        LPAD(FLOOR(RAND() * 28) + 1, 2, '0')
    ) as Time,
    CASE FLOOR(RAND() * 5)
        WHEN 0 THEN '交易金额异常增大，与客户身份和经济状况不符'
        WHEN 1 THEN '频繁进行跨境交易，资金来源不明'
        WHEN 2 THEN '与高风险地区有资金往来，存在洗钱风险'
        WHEN 3 THEN '交易模式异常，存在结构性存款特征'
        ELSE '客户身份信息存疑，需要进一步核实'
    END as Risk_reason,
    CASE FLOOR(RAND() * 5)
        WHEN 0 THEN '张明'
        WHEN 1 THEN '李华'
        WHEN 2 THEN '王强'
        WHEN 3 THEN '刘洋'
        ELSE '陈涛'
    END as Evaluator
FROM tb_cst_pers
WHERE CAST(SUBSTRING(Cst_no, 2) AS UNSIGNED) <= 100 -- 前100个个人客户设为高风险
LIMIT 100;

-- 高风险企业客户 (10个)
INSERT INTO tb_risk_new (Cst_no, Acc_type, Risk_code, Risk_score, Time, Risk_reason, Evaluator)
SELECT
    Cst_no,
    '13' as Acc_type,
    '01' as Risk_code,
    85 + FLOOR(RAND() * 15) as Risk_score,
    CONCAT(
        '2020',
        LPAD(FLOOR(RAND() * 6) + 1, 2, '0'),
        LPAD(FLOOR(RAND() * 28) + 1, 2, '0')
    ) as Time,
    CASE FLOOR(RAND() * 5)
        WHEN 0 THEN '企业经营异常，资金流动与大额交易不匹配'
        WHEN 1 THEN '频繁与敏感地区发生交易，风险等级较高'
        WHEN 2 THEN '跨境贸易量与注册资本不符，存在异常'
        WHEN 3 THEN '企业实际控制人背景复杂，风险较高'
        ELSE '行业风险等级较高，需要重点关注'
    END as Risk_reason,
    CASE FLOOR(RAND() * 5)
        WHEN 0 THEN '张经理'
        WHEN 1 THEN '李经理'
        WHEN 2 THEN '王经理'
        WHEN 3 THEN '刘经理'
        ELSE '陈经理'
    END as Evaluator
FROM tb_cst_unit
WHERE CAST(SUBSTRING(Cst_no, 2) AS UNSIGNED) <= 10 -- 前10个企业客户设为高风险
LIMIT 10;

-- 其他风险等级客户分配
-- 中高风险客户 (220个)
INSERT INTO tb_risk_new (Cst_no, Acc_type, Risk_code, Risk_score, Time, Risk_reason, Evaluator)
SELECT
    Cst_no,
    '11' as Acc_type,
    '02' as Risk_code,
    60 + FLOOR(RAND() * 20) as Risk_score,
    CONCAT(
        '2020',
        LPAD(FLOOR(RAND() * 6) + 1, 2, '0'),
        LPAD(FLOOR(RAND() * 28) + 1, 2, '0')
    ) as Time,
    '交易行为略有异常，需要持续监控',
    '系统自动评估'
FROM tb_cst_pers
WHERE CAST(SUBSTRING(Cst_no, 2) AS UNSIGNED) BETWEEN 101 AND 320
LIMIT 220;

-- 中风险客户 (550个)
INSERT INTO tb_risk_new (Cst_no, Acc_type, Risk_code, Risk_score, Time, Risk_reason, Evaluator)
SELECT
    Cst_no,
    '11' as Acc_type,
    '03' as Risk_code,
    40 + FLOOR(RAND() * 20) as Risk_score,
    CONCAT(
        '2020',
        LPAD(FLOOR(RAND() * 6) + 1, 2, '0'),
        LPAD(FLOOR(RAND() * 28) + 1, 2, '0')
    ) as Time,
    '正常客户，常规风险评估',
    '系统自动评估'
FROM tb_cst_pers
WHERE CAST(SUBSTRING(Cst_no, 2) AS UNSIGNED) BETWEEN 321 AND 870
LIMIT 550;

-- 低风险客户 (130个)
INSERT INTO tb_risk_new (Cst_no, Acc_type, Risk_code, Risk_score, Time, Risk_reason, Evaluator)
SELECT
    Cst_no,
    '11' as Acc_type,
    '04' as Risk_code,
    10 + FLOOR(RAND() * 30) as Risk_score,
    CONCAT(
        '2020',
        LPAD(FLOOR(RAND() * 6) + 1, 2, '0'),
        LPAD(FLOOR(RAND() * 28) + 1, 2, '0')
    ) as Time,
    '风险等级较低，正常客户',
    '系统自动评估'
FROM tb_cst_pers
WHERE CAST(SUBSTRING(Cst_no, 2) AS UNSIGNED) BETWEEN 871 AND 1000
LIMIT 130;

-- 企业客户风险评估
INSERT INTO tb_risk_new (Cst_no, Acc_type, Risk_code, Risk_score, Time, Risk_reason, Evaluator)
SELECT
    Cst_no,
    '13' as Acc_type,
    CASE
        WHEN CAST(SUBSTRING(Cst_no, 2) AS UNSIGNED) <= 10 THEN '01' -- 高风险
        WHEN CAST(SUBSTRING(Cst_no, 2) AS UNSIGNED) <= 30 THEN '02' -- 中高风险
        WHEN CAST(SUBSTRING(Cst_no, 2) AS UNSIGNED) <= 70 THEN '03' -- 中风险
        ELSE '04' -- 低风险
    END as Risk_code,
    CASE
        WHEN CAST(SUBSTRING(Cst_no, 2) AS UNSIGNED) <= 10 THEN 85 + FLOOR(RAND() * 15)
        WHEN CAST(SUBSTRING(Cst_no, 2) AS UNSIGNED) <= 30 THEN 60 + FLOOR(RAND() * 20)
        WHEN CAST(SUBSTRING(Cst_no, 2) AS UNSIGNED) <= 70 THEN 40 + FLOOR(RAND() * 20)
        ELSE 10 + FLOOR(RAND() * 30)
    END as Risk_score,
    CONCAT(
        '2020',
        LPAD(FLOOR(RAND() * 6) + 1, 2, '0'),
        LPAD(FLOOR(RAND() * 28) + 1, 2, '0')
    ) as Time,
    '企业客户定期风险评估',
    '系统自动评估'
FROM tb_cst_unit
WHERE CAST(SUBSTRING(Cst_no, 2) AS UNSIGNED) > 10
LIMIT 90;

-- =====================================================
-- 6. 联网核查记录生成 (约1100条)
-- =====================================================
-- 为高风险客户生成联网核查记录
INSERT INTO tb_lwhc_log (Date, Cst_no, Name, Id_no, Id_type, Result, Operator)
SELECT
    CONCAT(
        '2020',
        LPAD(FLOOR(RAND() * 6) + 1, 2, '0'),
        LPAD(FLOOR(RAND() * 28) + 1, 2, '0')
    ) as Date,
    p.Cst_no,
    p.Acc_name as Name,
    p.Id_no,
    p.Id_type,
    CASE FLOOR(RAND() * 100)
        WHEN 0 THEN '15' -- 不存在
        WHEN 1 THEN '16' -- 其他
        WHEN 2 THEN '16' -- 其他
        WHEN 3 THEN '16' -- 其他
        WHEN 4 THEN '16' -- 其他
        WHEN 5 THEN '15' -- 不存在
        WHEN 6 THEN '14' -- 不匹配
        WHEN 7 THEN '14' -- 不匹配
        WHEN 8 THEN '14' -- 不匹配
        WHEN 9 THEN '14' -- 不匹配
        WHEN 10 THEN '15' -- 不存在
        ELSE '11' -- 一致且照片存在
    END as Result,
    CASE FLOOR(RAND() * 5)
        WHEN 0 THEN '张柜员'
        WHEN 1 THEN '李柜员'
        WHEN 2 THEN '王柜员'
        WHEN 3 THEN '刘柜员'
        ELSE '系统自动'
    END as Operator
FROM tb_risk_new r
JOIN tb_cst_pers p ON r.Cst_no = p.Cst_no
WHERE r.Risk_code = '01'
LIMIT 100;

-- 为其他客户生成联网核查记录
INSERT INTO tb_lwhc_log (Date, Cst_no, Name, Id_no, Id_type, Result, Operator)
SELECT
    CONCAT(
        '2020',
        LPAD(FLOOR(RAND() * 6) + 1, 2, '0'),
        LPAD(FLOOR(RAND() * 28) + 1, 2, '0')
    ) as Date,
    p.Cst_no,
    p.Acc_name as Name,
    p.Id_no,
    p.Id_type,
    CASE FLOOR(RAND() * 100)
        WHEN 0 THEN '12' -- 一致无照片
        WHEN 1 THEN '13' -- 一致照片错误
        WHEN 2 THEN '14' -- 不匹配
        WHEN 3 THEN '15' -- 不存在
        WHEN 4 THEN '16' -- 其他
        ELSE '11' -- 一致且照片存在
    END as Result,
    '系统自动' as Operator
FROM tb_cst_pers p
WHERE CAST(SUBSTRING(p.Cst_no, 2) AS UNSIGNED) > 100 -- 非高风险客户
LIMIT 1000;

-- =====================================================
-- 7. 交易数据生成 (10,000笔交易)
-- =====================================================
-- 生成基础交易记录
INSERT INTO tb_acc_txn (Cst_no, Self_acc_no, Date, Tstm, Cur, Org_amt, Rmb_amt, Opp_acc, Opp_name, Txn_type, Summary, Channel)
SELECT
    a.Cst_no,
    a.Self_acc_no,
    CONCAT(
        '2020',
        LPAD(FLOOR(RAND() * 6) + 1, 2, '0'), -- 1-6月
        LPAD(FLOOR(RAND() * 28) + 1, 2, '0')  -- 1-28日
    ) as Date,
    CONCAT(
        LPAD(FLOOR(RAND() * 24), 2, '0'),    -- 小时
        LPAD(FLOOR(RAND() * 60), 2, '0'),    -- 分钟
        LPAD(FLOOR(RAND() * 60), 2, '0'),    -- 秒
        LPAD(FLOOR(RAND() * 1000000), 6, '0') -- 微秒
    ) as Tstm,
    'CNY' as Cur,
    CASE FLOOR(RAND() * 10)
        WHEN 0 THEN ROUND(RAND() * 10000 + 100, 2)      -- 小额交易 100-10,000
        WHEN 1 THEN ROUND(RAND() * 50000 + 10000, 2)    -- 中额交易 10,000-50,000
        WHEN 2 THEN ROUND(RAND() * 100000 + 50000, 2)   -- 大额交易 50,000-100,000
        WHEN 3 THEN ROUND(RAND() * 500000 + 100000, 2)  -- 超大额交易 100,000-500,000
        ELSE ROUND(RAND() * 10000 + 1000, 2)           -- 普通交易 1,000-10,000
    END as Org_amt,
    CASE FLOOR(RAND() * 10)
        WHEN 0 THEN ROUND(RAND() * 10000 + 100, 2)
        WHEN 1 THEN ROUND(RAND() * 50000 + 10000, 2)
        WHEN 2 THEN ROUND(RAND() * 100000 + 50000, 2)
        WHEN 3 THEN ROUND(RAND() * 500000 + 100000, 2)
        ELSE ROUND(RAND() * 10000 + 1000, 2)
    END as Rmb_amt,
    CONCAT('OTH', LPAD(FLOOR(RAND() * 1000000), 6, '0')) as Opp_acc,
    CASE FLOOR(RAND() * 20)
        WHEN 0 THEN '张三'
        WHEN 1 THEN '李四'
        WHEN 2 THEN '王五'
        WHEN 3 THEN '赵六'
        WHEN 4 THEN '钱七'
        WHEN 5 THEN '孙八'
        WHEN 6 THEN '周九'
        WHEN 7 THEN '吴十'
        WHEN 8 THEN '郑十一'
        WHEN 9 THEN '王十二'
        WHEN 10 THEN '李十三'
        WHEN 11 THEN '张十四'
        WHEN 12 THEN '刘十五'
        WHEN 13 THEN '陈十六'
        WHEN 14 THEN '杨十七'
        WHEN 15 THEN '黄十八'
        WHEN 16 THEN '赵十九'
        WHEN 17 THEN '周二十'
        WHEN 18 THEN '吴二一'
        ELSE '郑二二'
    END as Opp_name,
    CASE FLOOR(RAND() * 10)
        WHEN 0 THEN '转账'
        WHEN 1 THEN '存款'
        WHEN 2 THEN '取款'
        WHEN 3 THEN '汇款'
        WHEN 4 THEN '消费'
        WHEN 5 THEN '代发'
        WHEN 6 THEN '代扣'
        WHEN 7 THEN '转账'
        WHEN 8 THEN '存款'
        ELSE '取款'
    END as Txn_type,
    CASE FLOOR(RAND() * 10)
        WHEN 0 THEN '工资收入'
        WHEN 1 THEN '经营收入'
        WHEN 2 THEN '投资收益'
        WHEN 3 THEN '日常消费'
        WHEN 4 THEN '大额消费'
        WHEN 5 THEN '转账汇款'
        WHEN 6 THEN '还款'
        WHEN 7 THEN '借款'
        WHEN 8 THEN '投资'
        ELSE '其他'
    END as Summary,
    CASE FLOOR(RAND() * 5)
        WHEN 0 THEN '网银'
        WHEN 1 THEN '手机银行'
        WHEN 2 THEN 'ATM'
        WHEN 3 THEN '柜面'
        ELSE '自助终端'
    END as Channel
FROM tb_acc a
CROSS JOIN (
    SELECT 1 as seq UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION
    SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10 UNION
    SELECT 11 UNION SELECT 12 UNION SELECT 13 UNION SELECT 14 UNION SELECT 15 UNION
    SELECT 16 UNION SELECT 17 UNION SELECT 18 UNION SELECT 19 UNION SELECT 20
) t
WHERE a.Acc_state = '1'  -- 只为正常账户生成交易
LIMIT 10000;

SELECT '交易数据生成完成！' AS Message;

-- =====================================================
-- 由于脚本过长，剩余表的数据生成将在下一个文件中
-- 8. 大额交易报告生成
-- 9. 可疑交易报告生成
-- 10. 跨境交易数据生成
-- 11. 其他相关表数据生成
-- =====================================================