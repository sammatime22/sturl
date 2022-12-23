/**
 * This is the main script that can be used to create the job queue table.
 * sammatime22, 2022
 */
CREATE TABLE JOB_QUEUE {
    id bigint NOT NULL AUTO_INCREMENT,
    uuid varchar(36) NOT NULL,
    job_type enum ('STORE', 'RETRIEVE') NOT NULL,
    url_of_interest varchar(500) NULL,
    insert_time timestamp NOT NULL DEFAULT(current_timestamp),
    completed boolean NOT NULL DEFAULT 0,
    complete_time timestamp NULL,
    PRIMARY KEY (id)
};