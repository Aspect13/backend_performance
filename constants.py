from tools import constants as c

JMETER_MAPPING = {
    "v5.5": {
        "container": f"getcarrier/perfmeter:{c.CURRENT_RELEASE}-5.5",
        "job_type": "perfmeter",
        "influx_db": "{{secret.jmeter_db}}"
    },
    "v5.4.1": {
        "container": f"getcarrier/perfmeter:{c.CURRENT_RELEASE}-5.4.1",
        "job_type": "perfmeter",
        "influx_db": "{{secret.jmeter_db}}"
    },
    "v5.3": {
        "container": f"getcarrier/perfmeter:{c.CURRENT_RELEASE}-5.3",
        "job_type": "perfmeter",
        "influx_db": "{{secret.jmeter_db}}"
    },
    "v5.2.1": {
        "container": f"getcarrier/perfmeter:{c.CURRENT_RELEASE}-5.2.1",
        "job_type": "perfmeter",
        "influx_db": "{{secret.jmeter_db}}"
    },
    "v5.2": {
        "container": f"getcarrier/perfmeter:{c.CURRENT_RELEASE}-5.2",
        "job_type": "perfmeter",
        "influx_db": "{{secret.jmeter_db}}"
    },
    "v5.1.1": {
        "container": f"getcarrier/perfmeter:{c.CURRENT_RELEASE}-5.1.1",
        "job_type": "perfmeter",
        "influx_db": "{{secret.jmeter_db}}"
    },
    "v5.1": {
        "container": f"getcarrier/perfmeter:{c.CURRENT_RELEASE}-5.1",
        "job_type": "perfmeter",
        "influx_db": "{{secret.jmeter_db}}"
    },
    "v5.0": {
        "container": f"getcarrier/perfmeter:{c.CURRENT_RELEASE}-5.0",
        "job_type": "perfmeter",
        "influx_db": "{{secret.jmeter_db}}"
    },
    "v4.0": {
        "container": f"getcarrier/perfmeter:{c.CURRENT_RELEASE}-4.0",
        "job_type": "perfmeter",
        "influx_db": "{{secret.jmeter_db}}"
    },
}

GATLING_MAPPING = {
    "v3.7": {
        "container": f"getcarrier/perfgun:{c.CURRENT_RELEASE}-3.7",
        "job_type": "perfgun",
        "influx_db": "{{secret.gatling_db}}"
    },
    "v3.6": {
        "container": f"getcarrier/perfgun:{c.CURRENT_RELEASE}-3.6",
        "job_type": "perfgun",
        "influx_db": "{{secret.gatling_db}}"
    },
    "v3.1": {
        "container": f"getcarrier/perfgun:{c.CURRENT_RELEASE}-3.1",
        "job_type": "perfgun",
        "influx_db": "{{secret.gatling_db}}"
    },
    "v2.3": {
        "container": f"getcarrier/perfgun:{c.CURRENT_RELEASE}-2.3",
        "job_type": "perfgun",
        "influx_db": "{{secret.gatling_db}}"
    }
}

if c.LOCAL_DEV:
    JMETER_MAPPING['local_jmeter'] = {
        "container": "perfmeter:local",
        "job_type": "perfmeter",
        "influx_db": "{{secret.jmeter_db}}"
    }
    GATLING_MAPPING['local_gatling'] = {
        "container": "perfgun:local",
        "job_type": "perfgun",
        "influx_db": "{{secret.gatling_db}}"
    }

EXECUTABLE_MAPPING = {
    "gatling": {
        "container": f"getcarrier/executable_jar_runner:{c.CURRENT_RELEASE}-gatling",
        "job_type": "perfgun",
        "influx_db": "{{secret.gatling_db}}"
    },
    "base (in development)": {
        "container": f"getcarrier/executable_jar_runner:{c.CURRENT_RELEASE}-base",
        "job_type": "perfgun",
        "influx_db": "{{secret.gatling_db}}"
    }
}

JOB_CONTAINER_MAPPING = {
    **JMETER_MAPPING,
    **GATLING_MAPPING,
    **EXECUTABLE_MAPPING
}

JOB_TYPE_MAPPING = {
    "perfmeter": "jmeter",
    "perfgun": "gatling",
    "free_style": "other",
    "observer": "observer",
    "dast": "dast",
    "sast": "sast",
}
