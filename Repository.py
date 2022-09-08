from abc import abstractmethod


class Repository:
    @abstractmethod
    def add_all(self, items: list):
        pass

    @abstractmethod
    def add(self, item: dict):
        pass

    @abstractmethod
    def get_all(self) -> list:
        pass

    @abstractmethod
    def get(self, element_id) -> dict:
        pass
