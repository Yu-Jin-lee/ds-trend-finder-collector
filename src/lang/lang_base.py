from abc import ABCMeta, abstractmethod

class LanguageBase(metaclass=ABCMeta):
    
    @abstractmethod
    def language(self) -> str: 
        pass

    @property
    def hl(self) -> str:
        pass
    
    @property
    def gl(self) -> str:
        pass

    @abstractmethod
    def get_characters(self) -> list:
        pass

    @abstractmethod
    def get_letters(self) -> list:
        pass

    @abstractmethod
    def get_alphabets(self) -> list:
        pass

    @abstractmethod
    def get_numbers(self) -> list:
        pass
    
    @abstractmethod
    def suggest_extension_texts(self, stretegy) -> list:
        pass
    
    def interval_map(self) -> dict:
        ...