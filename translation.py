from google.cloud import translate

class Client:
    def __init__(source_language='en', target_language=None, format_='text', model='nmt'):
        self.source_language = source_language
        self.target_language = target_language
        self.format = format_
        self.model = model
        self.client = translate.Client(target_language=self.target_language)

    def translate(values):
        return self.client.translate(
            values,
            target_language=self.target_language,
            format_=self.format,
            source_language=self.source_language,
            model=self.model,
        )
