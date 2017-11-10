from google.cloud import translate

class CachingClient:
    def __init__(self, translator, cache):
        self.translator = translator
        self.cache = cache

    def translate(self, values):
        found, translation = self.cache.get(values)
        if found:
            return translation
        translated = self.translator.translate(values)
        self.cache.add(values, translated)
        return translated

    # TODO: Make this compatible with Python's `with` statement
    def close(self):
        self.cache.close()


class Client:
    MAX_LENGTH = 5000
    def __init__(self, source_language='en', target_language=None, format_='text', model='nmt'):
        self.source_language = source_language
        self.target_language = target_language
        self.format = format_
        self.model = model
        self.client = translate.Client(target_language=self.target_language)

    def translate(self, values):
        strings_to_translate = values if isinstance(values, list) else self.chunks(values)
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

    def chunks(self, values):
        return [values[i:i+Client.MAX_LENGTH] for i in range(0, len(values), Client.MAX_LENGTH)]
