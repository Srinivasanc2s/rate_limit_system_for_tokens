
CREATE DATABASE IF NOT EXISTS test_user1;
USE test_user1;

-- IMPORTANT: disable FK checks so drops never fail
SET FOREIGN_KEY_CHECKS = 0;

DROP TABLE IF EXISTS user_usage;
DROP TABLE IF EXISTS query_log;
DROP TABLE IF EXISTS users_table;
DROP TABLE IF EXISTS plans;

SET FOREIGN_KEY_CHECKS = 1;

-- =========================
-- TABLES (correct order)
-- =========================

CREATE TABLE plans (
    plan_id INT PRIMARY KEY,
    plan_name VARCHAR(50),
    token_limit INT,
    hourly_token_limit INT
);

CREATE TABLE users_table (
    user_id INT PRIMARY KEY,
    user_name VARCHAR(50),
    plan_id INT,
    FOREIGN KEY (plan_id) REFERENCES plans(plan_id)
);

CREATE TABLE query_log (
    query_id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT,
    query_content TEXT,
    tokens_used INT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users_table(user_id)
);

CREATE TABLE user_usage (
    user_id INT PRIMARY KEY,
    cumulative_tokens INT DEFAULT 0,
    hourly_tokens INT DEFAULT 0,
    hourly_session_start TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users_table(user_id)
);

-- =========================
-- SEED DATA
-- =========================

INSERT INTO plans (plan_id, plan_name, token_limit, hourly_token_limit)
VALUES 
(1, 'Basic Plan', 100000, 5000),
(2, 'Pro Plan', 500000, 20000);

INSERT INTO users_table (user_id, user_name, plan_id)
VALUES 
(101, 'John Doe', 1),
(102, 'Jane Max', 2);

-- =========================
-- VERIFY
-- =========================

SELECT * FROM plans;
SELECT * FROM users_table;



DELIMITER //

CREATE PROCEDURE process_user_query(
    IN p_user_id INT,
    IN p_query_content TEXT,
    IN p_tokens_used INT
)
BEGIN

    DECLARE v_total_limit INT;
    DECLARE v_hourly_limit INT;
    DECLARE v_cumulative_tokens INT DEFAULT 0;
    DECLARE v_hourly_tokens INT DEFAULT 0;
    DECLARE v_session_start TIMESTAMP;
    DECLARE v_is_new_session INT DEFAULT 0;

    -- STEP 1: Fetch user's plan limits
    SELECT p.token_limit, p.hourly_token_limit
    INTO v_total_limit, v_hourly_limit
    FROM users_table u
    JOIN plans p ON u.plan_id = p.plan_id
    WHERE u.user_id = p_user_id;

    -- STEP 2: Fetch current usage
    SELECT cumulative_tokens, hourly_tokens, hourly_session_start
    INTO v_cumulative_tokens, v_hourly_tokens, v_session_start
    FROM user_usage
    WHERE user_id = p_user_id;

    -- Handle first-time users
    IF v_cumulative_tokens IS NULL THEN
        SET v_cumulative_tokens = 0;
        SET v_hourly_tokens = 0;
    END IF;

    -- STEP 3: Check hourly session
    IF v_session_start IS NULL 
       OR CURRENT_TIMESTAMP > DATE_ADD(v_session_start, INTERVAL 1 HOUR) THEN

        SET v_is_new_session = 1;
        SET v_hourly_tokens = 0;

    END IF;

    -- STEP 4: Validate limits
    IF (v_cumulative_tokens + p_tokens_used) > v_total_limit THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'Total lifetime token limit exceeded.';
    END IF;

    IF (v_hourly_tokens + p_tokens_used) > v_hourly_limit THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'Hourly token limit exceeded.';
    END IF;

    -- STEP 5: Log query
    INSERT INTO query_log (
        user_id,
        query_content,
        tokens_used
    )
    VALUES (
        p_user_id,
        p_query_content,
        p_tokens_used
    );

    -- STEP 6: Update usage
    IF v_is_new_session = 1 THEN

        INSERT INTO user_usage (
            user_id,
            cumulative_tokens,
            hourly_tokens,
            hourly_session_start
        )
        VALUES (
            p_user_id,
            v_cumulative_tokens + p_tokens_used,
            p_tokens_used,
            CURRENT_TIMESTAMP
        )

        ON DUPLICATE KEY UPDATE
            cumulative_tokens = cumulative_tokens + p_tokens_used,
            hourly_tokens = p_tokens_used,
            hourly_session_start = CURRENT_TIMESTAMP;

    ELSE

        UPDATE user_usage
        SET cumulative_tokens = cumulative_tokens + p_tokens_used,
            hourly_tokens = hourly_tokens + p_tokens_used
        WHERE user_id = p_user_id;

    END IF;

END //

DELIMITER ;

CALL process_user_query(101, 'Hello world query', 120);

SELECT * FROM query_log;
SELECT * FROM user_usage;
