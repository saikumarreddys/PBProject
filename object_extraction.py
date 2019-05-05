import json


with open("tweets_final1.txt", 'r') as inputfile:
    # Extracting hashtags from tweets
    hash_file = open("hashtags1.txt", 'a')
    for line in inputfile:
        try:
            ob = {}
            #ob['country'] = json.loads(line).get('place').get('country')
            #ob['PostedTime'] = json.loads(line).get('user').get('created_at')
            #ob['RetweetCount'] = json.loads(line).get('retweeted_status').get('retweet_count')
            #ob['ScreenName'] = json.loads(line).get('user').get('screen_name')
            #ob['Hashtag'] = json.loads(line).get('entities').get('hashtags')
            #ob['Verified'] = json.loads(line).get('user').get('verified')
            #hashdata = json.loads(line).get('entities').get('hashtags')
            #for obj in hashdata:
            #ob['Hashtags'] = (obj.get('text') + ' ')

               # print(ob)
            #hash_file.write(json.dumps(ob)+"\n")
            #ob['timestamp'] = json.loads(line).get('timestamp_ms')
            ob['createdat'] = json.loads(line).get('created_at')
            hash_file.write(json.dumps(ob) + "\n")
        except BaseException as e:
            continue
    hash_file.close()



