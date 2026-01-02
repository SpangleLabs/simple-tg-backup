/*
 This migration merges the archive history timer and the follow live timer columns of the archive_run record table.
 This is more complex than a lot of migrations, because it needs some data merging too.
 */


-- First, add the new columns
alter table archive_runs
    add run_time_start text;
alter table archive_runs
    add run_time_latest text;
alter table archive_runs
    add run_time_end text;

-- Second, Use the latest or earliest, when both timers are set. (Earliest start, latest end/latest)
UPDATE archive_runs
    SET run_time_start = history_time_start
    WHERE history_time_start IS NOT NULL AND follow_time_start IS NOT NULL AND history_time_start < follow_time_start;
UPDATE archive_runs
    SET run_time_start = follow_time_start
    WHERE history_time_start IS NOT NULL AND follow_time_start IS NOT NULL AND history_time_start > follow_time_start;
UPDATE archive_runs
    SET run_time_latest = follow_time_latest
    WHERE history_time_latest IS NOT NULL AND follow_time_latest IS NOT NULL AND history_time_latest < follow_time_latest;
UPDATE archive_runs
    SET run_time_latest = history_time_latest
    WHERE history_time_latest IS NOT NULL AND follow_time_latest IS NOT NULL AND history_time_latest > follow_time_latest;
UPDATE archive_runs
    SET run_time_end = follow_time_end
    WHERE history_time_end IS NOT NULL AND follow_time_end IS NOT NULL AND history_time_end < follow_time_end;
UPDATE archive_runs
    SET run_time_end = history_time_end
    WHERE history_time_end IS NOT NULL AND follow_time_end IS NOT NULL AND history_time_end > follow_time_end;

-- Third, use the archive_history timer value when only the archive_history timer value is set
UPDATE archive_runs
    SET run_time_start = history_time_start
    WHERE follow_time_start IS NULL;
UPDATE archive_runs
    SET run_time_latest = history_time_latest
    WHERE follow_time_latest IS NULL;
UPDATE archive_runs
    SET run_time_end = history_time_end
    WHERE follow_time_end IS NULL;

-- Fourth, use the follow_live timer value when only the follow_live timer value is set
UPDATE archive_runs
    SET run_time_start = follow_time_start
    WHERE history_time_start IS NULL;
UPDATE archive_runs
    SET run_time_latest = follow_time_latest
    WHERE history_time_latest IS NULL;
UPDATE archive_runs
    SET run_time_end = follow_time_end
    WHERE history_time_end IS NULL;

-- (In cases where both are null, then the new timer already has the right value)
-- Fifth, delete the redundant columns
CREATE TABLE archive_runs_dg_tmp
(
    archive_run_id   text    not null
        constraint table_name_pk
            primary key,
    target_type      text    not null,
    target_id        integer not null,
    time_queued      text,
    run_time_start   text,
    run_time_latest  text,
    run_time_end     text,
    behaviour_config text,
    completed        boolean,
    failure_reason   text,
    archive_stats    text
);

INSERT INTO archive_runs_dg_tmp(archive_run_id, target_type, target_id, time_queued, run_time_start, run_time_latest, run_time_end, behaviour_config, completed, failure_reason, archive_stats)
    SELECT archive_run_id, target_type, target_id, time_queued, run_time_start, run_time_latest, run_time_end, behaviour_config, completed, failure_reason, archive_stats
    FROM archive_runs;

DROP TABLE archive_runs;

ALTER TABLE archive_runs_dg_tmp
    RENAME TO archive_runs;
