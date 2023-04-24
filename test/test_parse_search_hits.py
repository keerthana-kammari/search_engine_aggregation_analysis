import unittest
from urllib.parse import parse_qs
from scripts.parse_search_hits_script import parse_url

class TestParseUrl(unittest.TestCase):
    
    def test_google_url(self):
        url = "http://www.google.com/search?hl=en&client=firefox-a&rls=org.mozilla%3Aen-US%3Aofficial&hs=ZzP&q=Ipod&aq=f&oq=&aqi="
        expected_result = ("google", "Ipod")
        self.assertEqual(parse_url(url), expected_result)
    
    def test_bing_url(self):
        url = "http://www.bing.com/search?q=Zune&go=&form=QBLH&qs=n"
        expected_result = ("bing", "Zune")
        self.assertEqual(parse_url(url), expected_result)
    
    def test_missing_query_param(self):
        url = "http://search.yahoo.com/search?ei=UTF-8&fr=yfp-t-701"
        expected_result = ("yahoo", "")
        self.assertEqual(parse_url(url), expected_result)
    
    def test_multiple_query_params(self):
        url = "http://www.google.com/search?q=Ipod&aq=f&oq=&aqi="
        expected_result = ("google", "Ipod")
        self.assertEqual(parse_url(url), expected_result)
    
    def test_empty_url(self):
        url = ""
        expected_result = ("", "")
        self.assertEqual(parse_url(url), expected_result)

if __name__ == '__main__':
    unittest.main()
