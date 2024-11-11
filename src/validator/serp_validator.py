class SerpValidator:
    def __init__(self, 
                 serp: dict):
        self.serp = serp

    def validate(self) -> bool:
        if ('features' in self.serp):
            return True
            
        return False