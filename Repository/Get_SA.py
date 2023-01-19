import Environment as envi


class SA:
    def __init__(self, project_id):
        # os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key
        self.service_account = ''
        self.project_id = ''

        if 'Project_A' in project_id:
            self.service_account = envi.A_Key
            self.project_id = envi.Project_A

        elif 'Project_B' in project_id:
            self.service_account = envi.B_Key
            self.project_id = envi.Project_B

        elif 'Project_C' in project_id:
            self.service_account = envi.C_Key
            self.project_id = envi.Project_C
