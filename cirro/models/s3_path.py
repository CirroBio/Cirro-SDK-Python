from urllib.parse import urlparse


class S3Path:
    def __init__(self, url):
        self._parsed = urlparse(url, allow_fragments=False)

    @property
    def bucket(self):
        return self._parsed.netloc

    @property
    def key(self):
        return self._parsed.path.lstrip('/')

    @property
    def valid(self):
        return self._parsed.scheme == 's3'

    @property
    def base(self):
        return f's3://{self.bucket}'

    def __str__(self):
        return self._parsed.geturl()

    def __eq__(self, other):
        if isinstance(other, S3Path):
            return self.key == other.key
        return NotImplemented

    def __hash__(self):
        return hash(self.key)
