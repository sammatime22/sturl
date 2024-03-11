/**
 * This is the main script that can be used to create the job queue table.
 * sammatime22, 2024
 */
CREATE TABLE JOB_QUEUE {
    uuid varchar(36) NOT NULL,
    url_of_interest varchar(500) NOT NULL,
    insert_time timestamp NOT NULL DEFAULT(current_timestamp),
    inserter_ip varchar(15) NOT NULL,
    inserter_mac varchar(17) NOT NULL,
    completed boolean NOT NULL DEFAULT 0,
    complete_time timestamp NULL,
    tasked_resource varchar(36) NULL,
    PRIMARY KEY (uuid)
};

CREATE TABLE DATA_WORKERS {
    resource_id SMALLINT(4) NOT NULL,
    tasked boolean NOT NULL DEFAULT 0,
    current_task varchar(36) NOT NULL,
    updated_at timestamp NOT NULL DEFAULT(current_timestamp),
    PRIMARY KEY (resource_id)
};
