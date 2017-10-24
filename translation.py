from google.cloud import translate

class Client:
    def __init__(self, source_language='en', target_language=None, format_='text', model='nmt'):
        self.source_language = source_language
        self.target_language = target_language
        self.format = format_
        self.model = model
        self.client = translate.Client(target_language=self.target_language)

    def translate(self, values):
        strings_to_translate = values if isinstance(values, list) else [values]
        if self.source_language == 'en' and self.source_language == self.target_language:
            return [
                dict(
                    detectedSourceLanguage=self.source_language,
                    model='nop',
                    translatedText=v
                ) for v in strings_to_translate
            ]
        return self.client.translate(
            values,
            target_language=self.target_language,
            format_=self.format,
            source_language=self.source_language,
            model=self.model,
        )
