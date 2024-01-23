from extruct import JsonLdExtractor

class DefaultHandler:
    def __init__(self):
        self.ld = JsonLdExtractor()

    def filter(self, html):
        if not html:
            return None

        schema = None

        try:
            schema = self.ld.extract(html, encoding="utf-8")
        except Exception as e: # ignore broken metadata
            pass

        if not schema:
            return None
        
        return schema
