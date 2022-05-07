import requests
import auth
class DataHandle:
    def url_builder(self, query, start_time, end_time, max_count, pagination_token):
        url = "https://api.twitter.com/2/tweets/search/recent?query=" + query + "&start_time=" + start_time
        if(end_time): url += "&end_time=" + end_time
        url+="&max_results=" + max_count
        if(pagination_token): url +=  "&pagination_token=" + pagination_token
        url += "&expansions=author_id,referenced_tweets.id,geo.place_id,entities.mentions.username&tweet.fields=id,created_at,text,author_id,in_reply_to_user_id,geo,entities,public_metrics,possibly_sensitive,source,lang,conversation_id&user.fields=created_at,name,username,verified,profile_image_url,location,url,description,entities,public_metrics"
        return url
    
    def connect_to_endpoint(self, url):
        headers = {
            'Authorization': 'Bearer ' + auth.creds['BEARER_TOKEN'],
            'Cookie': 'guest_id=v1%3A165046481231790839; guest_id_ads=v1%3A165046481231790839; guest_id_marketing=v1%3A165046481231790839; personalization_id="v1_k4Od+og0R+gl1oWMfH3U+A=="'
        }

        response = requests.request("GET", url, headers=headers)
        print(response.status_code)
        if response.status_code != 200:
            raise Exception(response.status_code, response.text)
        return response.json()

    def get_tweets(self, url):
        data = self.connect_to_endpoint(url)
        return data