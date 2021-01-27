import json

from pyfacebook import Api, IgProApi

api = IgProApi(
    app_id="",
    app_secret="",
    long_term_token="",
    instagram_business_id="",
)
print(api.get_token_info())
release_id = api.search_hashtag(q="release")
print(release_id[0].id)


# hashtag_info = api.get_hashtag_info(hashtag_id=release_id[0].id)
# print(hashtag_info.id)


for i in api.get_hashtag_recent_medias(hashtag_id=release_id[0].id, count=90):
    # fields="media_url, media_type, caption, comments_count, permalink",
    print(i.id)
    # print(i.caption)
