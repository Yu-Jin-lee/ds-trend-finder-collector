alphabet_list = [chr(i) for i in range(ord('a'), ord('z')+1)]


import string
from lang.lang_base import LanguageBase

class En(LanguageBase):
    def __init__(self) -> None:
        super().__init__()
        self.alphabets = self.get_alphabets()
        self.numbers = self.get_numbers()
        self.letters = self.get_letters()
    
    def language(self) -> str: 
        return 'en'

    @property
    def hl(self) -> str:
        return 'en'

    @property
    def gl(self) -> str:
        return 'us'
    
    def serp_location(self) -> str:
        return 'United States'
    
    def get_none(self) -> list:
        return [""]
    
    def get_characters(self) -> list:
        pass

    def get_letters(self) -> list:
        pass
    
    def get_alphabets(self) -> list:
        return list(string.ascii_lowercase)

    def get_numbers(self) -> list:
        return [str(n) for n in list(range(0,10))]
    
    def suggest_extension_texts_by_rank(self, rank) -> list:
        if rank == 0:
            return self.get_none()
        elif rank == 1:
            return self.get_alphabets() + self.get_numbers()
        elif rank == 2:
            targets = self.get_alphabets() + self.get_numbers()
            return [x + y \
                    for x in targets \
                    for y in targets
                    ]
        elif rank == 3:
            targets = self.get_alphabets() + self.get_numbers()
            return [x + y + z \
                    for x in targets \
                    for y in targets \
                    for z in targets
                    ]
        elif rank == 4:
            targets = self.get_alphabets() + self.get_numbers()
            return [x + y + z + a \
                    for x in targets \
                    for y in targets \
                    for z in targets \
                    for a in targets
                    ]
            
    def suggest_extension_texts(self, 
                                stratgy : str = "all",
                                contain_none : bool = False) -> list:
        if stratgy == "all":
            if contain_none:
                return self.get_none() + \
                       self.get_alphabets() + \
                       self.get_numbers()
            else:
                return self.get_alphabets() + \
                       self.get_numbers()

    def interval_map(self):
        i_map = {'d':' days ago', 'h':' hours ago', 'm':' minutes ago'}
        return i_map
    
if __name__ == "__main__":
    en = En()
    print(en.hl, en.gl)
    print(len(en.get_alphabets()),
          en.get_alphabets())
    