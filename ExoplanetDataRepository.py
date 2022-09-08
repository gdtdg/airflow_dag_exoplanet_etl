import requests

from Repository import Repository


class ExoplanetDataRepository(Repository):
    def __init__(self, api_url):
        self.api_url = api_url

    def get_all(self, ) -> list:
        response = requests.get(self.api_url, headers={'Accept': 'application/json'})
        data = response.content.decode("utf-8").splitlines()
        lines = []
        for line in data:
            if not line.startswith("#"):
                lines.append(line)

        header_list = lines[0].split(',')
        del lines[0]
        data = []
        for line_number in range(len(lines)):
            current_dict = {}
            for column_number in range(len(header_list)):
                values = lines[line_number].split(",")
                current_dict[header_list[column_number]] = values[column_number]
            data.append(current_dict)
        return data
