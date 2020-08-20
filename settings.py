class Development:
    conf = "mssql+pymssql://sa:Imanity.568312@127.0.0.1/Environment"
    userId = "E4CE162A-8D0B-451F-B7B9-0D99CA4F5BB8"


class Production:
    conf = "mssql+pymssql://sa:jkenvc2020@127.0.0.1/Environment"


config = {
    "dev": Development(),
    "pro": Production(),
}


def settings():
    return config["dev"]
