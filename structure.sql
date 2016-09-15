-- Create syntax for TABLE 'ActionLog'
CREATE TABLE `ActionLog` (
 `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
 `employee_id` int(11) NOT NULL,
 `action` varchar(30) COLLATE utf8_unicode_ci NOT NULL,
 `class_name` varchar(150) COLLATE utf8_unicode_ci NOT NULL DEFAULT '',
 `comment` varchar(1024) COLLATE utf8_unicode_ci DEFAULT NULL,
 `created` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
 PRIMARY KEY (`id`),
 KEY `class_created` (`class_name`,`created`),
 KEY `created` (`created`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

-- Create syntax for TABLE 'ActionScriptSettings'
CREATE TABLE `ActionScriptSettings` (
 `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
 `settings_id` bigint(20) unsigned NOT NULL,
 `action_id` int(11) unsigned NOT NULL,
 `type` enum('new','diff','delete') NOT NULL DEFAULT 'new',
 PRIMARY KEY (`id`),
 KEY `action_id` (`action_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- Create syntax for TABLE 'AllRunToken'
CREATE TABLE `AllRunToken` (
 `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
 `token` varchar(255) COLLATE utf8_unicode_ci NOT NULL DEFAULT '',
 `author` varchar(50) COLLATE utf8_unicode_ci NOT NULL DEFAULT '',
 PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

-- Create syntax for TABLE 'AllScripts'
CREATE TABLE `AllScripts` (
 `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
 `class_name` varchar(150) COLLATE utf8_unicode_ci NOT NULL DEFAULT '',
 `created` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
 `updated` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
 PRIMARY KEY (`id`),
 UNIQUE KEY `class_name` (`class_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

-- Create syntax for TABLE 'GlobalScriptRusage'
CREATE TABLE `GlobalScriptRusage` (
 `class_name` varchar(150) NOT NULL DEFAULT '',
 `interv` datetime NOT NULL,
 `cnt` int(11) NOT NULL,
 `rusage` float NOT NULL,
 `real_time` float NOT NULL,
 `max_memory` bigint(11) NOT NULL,
 PRIMARY KEY (`class_name`,`interv`),
 KEY `interv` (`interv`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- Create syntax for TABLE 'Locks'
CREATE TABLE `Locks` (
 `name` varchar(50) COLLATE utf8_unicode_ci NOT NULL DEFAULT '',
 `hostname` varchar(50) COLLATE utf8_unicode_ci NOT NULL DEFAULT '',
 `created` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
 `updated` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
 `updated_ns` int(11) unsigned NOT NULL,
 PRIMARY KEY (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

-- Create syntax for TABLE 'PerfTest'
CREATE TABLE `PerfTest` (
 `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
 `latency` float NOT NULL,
 `created` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
 PRIMARY KEY (`id`),
 KEY `created` (`created`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- Create syntax for TABLE 'ResourceShortageLog'
CREATE TABLE `ResourceShortageLog` (
 `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
 `location` varchar(255) COLLATE utf8_unicode_ci NOT NULL DEFAULT '',
 `stats` mediumtext COLLATE utf8_unicode_ci NOT NULL,
 `created` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
 PRIMARY KEY (`id`),
 KEY `created` (`created`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

-- Create syntax for TABLE 'ResourceShortageLog_old'
CREATE TABLE `ResourceShortageLog_old` (
 `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
 `location` varchar(255) COLLATE utf8_unicode_ci NOT NULL DEFAULT '',
 `stats` mediumtext COLLATE utf8_unicode_ci NOT NULL,
 `created` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
 PRIMARY KEY (`id`),
 KEY `created` (`created`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

-- Create syntax for TABLE 'RunQueue'
CREATE TABLE `RunQueue` (
 `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
 `timetable_id` bigint(20) unsigned DEFAULT NULL,
 `generation_id` bigint(20) unsigned DEFAULT NULL,
 `hostname` varchar(50) CHARACTER SET utf8 DEFAULT '',
 `hostname_idx` int(10) unsigned NOT NULL DEFAULT '0',
 `class_name` varchar(150) COLLATE utf8_unicode_ci NOT NULL DEFAULT '',
 `job_data` text CHARACTER SET utf8 NOT NULL,
 `method` enum('run','initJobs','finishJobs') CHARACTER SET utf8 NOT NULL DEFAULT 'run',
 `run_status` enum('Waiting','Init','Running','Finished') CHARACTER SET utf8 NOT NULL DEFAULT 'Waiting',
 `created` timestamp NULL DEFAULT NULL,
 `waiting_ts` timestamp NULL DEFAULT NULL,
 `should_init_ts` timestamp NULL DEFAULT NULL,
 `init_attempts` tinyint(3) unsigned NOT NULL DEFAULT '0',
 `init_ts` timestamp NULL DEFAULT NULL,
 `running_ts` timestamp NULL DEFAULT NULL,
 `max_finished_ts` timestamp NULL DEFAULT NULL,
 `finished_ts` timestamp NULL DEFAULT NULL,
 `stopped_employee_id` int(10) DEFAULT NULL,
 `token` varchar(100) COLLATE utf8_unicode_ci NOT NULL DEFAULT '',
 `retry_attempt` int(10) unsigned NOT NULL DEFAULT '0',
 `settings_id` bigint(20) unsigned NOT NULL,
 PRIMARY KEY (`id`),
 KEY `hostname_idx` (`hostname_idx`,`run_status`,`max_finished_ts`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

-- Create syntax for TABLE 'Script'
CREATE TABLE `Script` (
 `class_name` varchar(150) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL DEFAULT '',
 `is_system` tinyint(1) unsigned NOT NULL DEFAULT '0',
 `created` timestamp NULL DEFAULT NULL,
 `updated` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
 `team` varchar(50) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
 `team_email` varchar(100) DEFAULT NULL,
 `settings_id` bigint(20) unsigned DEFAULT NULL,
 PRIMARY KEY (`class_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- Create syntax for TABLE 'ScriptFailInfo'
CREATE TABLE `ScriptFailInfo` (
 `class_name` varchar(150) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL DEFAULT '',
 `email_send_ts` timestamp NULL DEFAULT NULL,
 `error_email_send_ts` timestamp NULL DEFAULT NULL,
 `paused_email_send_ts` timestamp NULL DEFAULT NULL,
 `updated` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
 PRIMARY KEY (`class_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- Create syntax for TABLE 'ScriptFlags'
CREATE TABLE `ScriptFlags` (
 `class_name` varchar(150) COLLATE utf8_unicode_ci NOT NULL,
 `run_requested_ts` timestamp NULL DEFAULT NULL,
 `run_accepted_ts` timestamp NULL DEFAULT NULL,
 `pause_requested_ts` timestamp NULL DEFAULT NULL,
 `kill_requested_ts` timestamp NULL DEFAULT NULL,
 `kill_request_employee_id` int(11) DEFAULT NULL,
 `run_queue_killed_ts` timestamp NULL DEFAULT NULL,
 `killed_ts` timestamp NULL DEFAULT NULL,
 `paused_ts` timestamp NULL DEFAULT NULL,
 `updated` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
 PRIMARY KEY (`class_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

-- Create syntax for TABLE 'ScriptJobInfo'
CREATE TABLE `ScriptJobInfo` (
 `generation_id` bigint(20) unsigned NOT NULL DEFAULT '0',
 `class_name` varchar(150) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL DEFAULT '',
 `location` varchar(255) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL DEFAULT '0',
 `init_jobs_ts` timestamp NULL DEFAULT NULL,
 `jobs_generated_ts` timestamp NULL DEFAULT NULL,
 `finish_jobs_ts` timestamp NULL DEFAULT NULL,
 `jobs_finished_ts` timestamp NULL DEFAULT NULL,
 `next_generate_job_ts` timestamp NULL DEFAULT NULL,
 `stop_requested_ts` timestamp NULL DEFAULT NULL,
 `stopped_ts` timestamp NULL DEFAULT NULL,
 `updated` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
 `settings_id` bigint(20) unsigned NOT NULL,
 PRIMARY KEY (`class_name`,`location`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- Create syntax for TABLE 'ScriptJobResult'
CREATE TABLE `ScriptJobResult` (
 `id` bigint(11) unsigned NOT NULL AUTO_INCREMENT,
 `class_name` varchar(150) COLLATE utf8_unicode_ci NOT NULL,
 `location` varchar(255) COLLATE utf8_unicode_ci NOT NULL DEFAULT '0',
 `timetable_id` bigint(20) unsigned NOT NULL,
 `job_data` text COLLATE utf8_unicode_ci NOT NULL,
 `job_result` mediumtext COLLATE utf8_unicode_ci NOT NULL,
 PRIMARY KEY (`id`),
 UNIQUE KEY `timetable_id` (`timetable_id`),
 KEY `class_name` (`class_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

-- Create syntax for TABLE 'ScriptRusageStats'
CREATE TABLE `ScriptRusageStats` (
 `class_name` varchar(150) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL DEFAULT '',
 `real_time` float NOT NULL,
 `sys_time` float NOT NULL,
 `user_time` float NOT NULL,
 `max_memory` bigint(20) unsigned NOT NULL,
 `created` timestamp NULL DEFAULT NULL,
 `updated` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
 `avg_real_time` float NOT NULL,
 `success_rate` float NOT NULL DEFAULT '0',
 PRIMARY KEY (`class_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- Create syntax for TABLE 'ScriptSettings'
CREATE TABLE `ScriptSettings` (
 `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
 `class_name` varchar(150) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL DEFAULT '',
 `instance_count` int(10) unsigned NOT NULL,
 `max_time` int(10) unsigned NOT NULL,
 `jobs` text NOT NULL,
 `next_ts_callback` varchar(4096) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
 `repeat` int(10) unsigned NOT NULL,
 `retry` int(10) unsigned DEFAULT NULL,
 `ttl` int(10) unsigned NOT NULL,
 `repeat_job` int(10) unsigned DEFAULT NULL,
 `retry_job` int(10) unsigned NOT NULL,
 `location` varchar(255) NOT NULL,
 `location_type` enum('any','each','server') CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL DEFAULT 'any',
 `developer` varchar(100) DEFAULT NULL,
 `notify_team` tinyint(1) unsigned NOT NULL DEFAULT '0',
 `repair_issue` varchar(150) DEFAULT NULL,
 `max_retries` int(10) unsigned NOT NULL DEFAULT '6',
 `profiling_enabled` tinyint(1) NOT NULL DEFAULT '0',
 `debug_enabled` tinyint(1) NOT NULL DEFAULT '0',
 `custom_settings` text,
 `named_params` text,
 `shot` varchar(45) DEFAULT NULL,
 `created` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
 PRIMARY KEY (`id`),
 KEY `class_created_platform` (`class_name`,`created`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- Create syntax for TABLE 'ScriptTag'
CREATE TABLE `ScriptTag` (
 `class_name` varchar(150) COLLATE utf8_unicode_ci NOT NULL DEFAULT '',
 `tag_name` varchar(50) COLLATE utf8_unicode_ci NOT NULL DEFAULT '',
 PRIMARY KEY (`class_name`,`tag_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

-- Create syntax for TABLE 'ScriptTimetable'
CREATE TABLE `ScriptTimetable` (
 `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
 `generation_id` bigint(20) unsigned DEFAULT NULL,
 `class_name` varchar(150) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '',
 `repeat` int(10) unsigned DEFAULT NULL,
 `retry_count` int(10) unsigned NOT NULL DEFAULT '0',
 `default_retry` int(10) unsigned NOT NULL,
 `job_data` text NOT NULL,
 `method` enum('run','initJobs','finishJobs') NOT NULL DEFAULT 'run',
 `location` varchar(255) NOT NULL,
 `finished_ts` timestamp NULL DEFAULT NULL,
 `shard_id` int(11) NOT NULL,
 `finished_successfully` tinyint(1) NOT NULL,
 `finish_count` int(10) unsigned NOT NULL DEFAULT '0',
 `dispatch_threshold` int(10) unsigned DEFAULT NULL,
 `decay_period` int(10) unsigned DEFAULT NULL,
 `next_launch_ts` timestamp NULL DEFAULT NULL,
 `added_to_queue_ts` timestamp NULL DEFAULT NULL,
 `created` timestamp NULL DEFAULT NULL,
 `updated` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
 `token` varchar(100) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL DEFAULT '',
 `settings_id` bigint(20) unsigned NOT NULL,
 PRIMARY KEY (`id`),
 KEY `next_launch_at` (`next_launch_ts`),
 KEY `shard_id` (`shard_id`),
 KEY `class_name` (`class_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- Create syntax for TABLE 'Server'
CREATE TABLE `Server` (
 `hostname` varchar(50) NOT NULL DEFAULT '',
 `group` varchar(50) NOT NULL DEFAULT '',
 `cpu_user` float DEFAULT NULL,
 `cpu_sys` float DEFAULT NULL,
 `cpu_nice` float DEFAULT NULL,
 `cpu_iowait` float DEFAULT NULL,
 `cpu_steal` float DEFAULT NULL,
 `cpu_idle` float DEFAULT NULL,
 `cpu_parasite` float DEFAULT NULL,
 `cpu_parrots_per_core` int(10) unsigned NOT NULL DEFAULT '0',
 `cpu_cores` mediumint(8) unsigned NOT NULL DEFAULT '0',
 `mem_total` bigint(20) DEFAULT NULL,
 `mem_free` bigint(20) DEFAULT NULL,
 `mem_cached` bigint(20) DEFAULT NULL,
 `mem_buffers` bigint(20) DEFAULT NULL,
 `mem_parasite` bigint(20) DEFAULT NULL,
 `swap_total` bigint(20) DEFAULT NULL,
 `swap_used` bigint(20) DEFAULT NULL,
 `min_memory_ratio` float DEFAULT NULL,
 `updated` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
 `phproxyd_heartbeat_ts` timestamp NULL DEFAULT NULL,
 `disabled_ts` timestamp NULL DEFAULT NULL,
 `min_memory` bigint(20) DEFAULT NULL,
 `min_parrots` bigint(20) DEFAULT NULL,
 `disabled_idx` int(10) DEFAULT NULL,
 `weight` bigint(20) unsigned DEFAULT NULL,
 PRIMARY KEY (`hostname`),
 KEY `disabled_idx` (`disabled_idx`,`group`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- Create syntax for TABLE 'ServerGroup'
CREATE TABLE `ServerGroup` (
 `name` varchar(50) NOT NULL DEFAULT '',
 PRIMARY KEY (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- Create syntax for TABLE 'Status'
CREATE TABLE `Status` (
 `class_name` varchar(50) COLLATE utf8_unicode_ci NOT NULL DEFAULT '',
 `instance_idx` int(10) unsigned NOT NULL,
 `hostname` varchar(50) COLLATE utf8_unicode_ci NOT NULL DEFAULT '',
 `cycle_start_ts` timestamp NULL DEFAULT NULL,
 `cycle_stop_ts` timestamp NULL DEFAULT NULL,
 `success` int(11) NOT NULL DEFAULT '0',
 PRIMARY KEY (`class_name`,`instance_idx`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

-- Create syntax for TABLE 'Token'
CREATE TABLE `Token` (
 `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
 `class_name` varchar(150) COLLATE utf8_unicode_ci NOT NULL,
 `token` varchar(100) COLLATE utf8_unicode_ci NOT NULL DEFAULT '',
 `token_uniqid` int(11) unsigned NOT NULL,
 `created` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
 `updated` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
 PRIMARY KEY (`id`),
 UNIQUE KEY `class_name` (`class_name`,`token`),
 KEY `updated` (`updated`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

-- Create syntax for TABLE 'Weights'
CREATE TABLE `Weights` (
 `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
 `hostname` varchar(255) COLLATE utf8_unicode_ci NOT NULL DEFAULT '',
 `weight` int(11) unsigned NOT NULL,
 PRIMARY KEY (`id`),
 UNIQUE KEY `hostname` (`hostname`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
