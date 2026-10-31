-- One row represents one business award. event_id prevents awarding it twice.
CREATE TABLE IF NOT EXISTS awards (event_id VARCHAR(100) PRIMARY KEY, customer VARCHAR(100) NOT NULL, points INTEGER NOT NULL);
