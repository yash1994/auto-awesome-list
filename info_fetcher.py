import re
import os
import json
import requests
from datetime import datetime, timedelta
from dateutil import parser


def ngrams(s, n):
    s = s.lower()
    s = re.sub(r'[^a-zA-Z0-9\s]', ' ', s)
    tokens = [token for token in s.split(" ") if token != ""]
    ngrams = zip(*[tokens[i:] for i in range(n)])
    if n == 1:
        return tokens
    else:
        return [" ".join(ngram) for ngram in ngrams]


def generate_hyperlink(txt, url):
    return "[{}]({})".format(txt, url)


class Writer:
    def __init__(self):
        self.data_directory = "data/"
        self.pwd = os.path.dirname(os.path.realpath(__file__))

        self.data_directory_path = os.path.join(self.pwd, self.data_directory)

        if not os.path.exists(self.data_directory_path):
            os.mkdir(self.data_directory_path)

        self.ext = ".md"
        self.header = "# n_org" + "\n\n" + "name_org currently holds n_total_repos public repositories out of which " \
                                           "n_repos are related to data science and machine learning." + "\n\n "

        self.new_arrival_blob = "## Newly Added" + "\n\n" + "| Name | Description | Language | Stars | License |" + \
                                "\n" \
                                + "| ---- | ----------- | :--------: | :-----: | :-------: |" + "\n"

        self.old_is_gold_blob = "## Highly Rated" + "\n\n" + "| Name | Description | Language | Stars | License |" + \
                                "\n" + \
                                "| ---- | ----------- | :--------: | :-----: | :-------: |" + "\n "

    def write_information(self, org_name, high_rated_repos, newly_added_repos, no_of_total_repos, url):

        with open(os.path.join(self.data_directory_path, org_name.lower() + self.ext), "w") as outfile:
            text = self.header.replace("n_org", generate_hyperlink(org_name, url)).\
                replace("n_total_repos", str(no_of_total_repos)). \
                replace("n_repos", str(len(high_rated_repos) + len(newly_added_repos))). \
                replace("name_org", org_name)

            if newly_added_repos:
                text += self.new_arrival_blob
                for nar in newly_added_repos:
                    text += "| {} | {} | {} | {} | {} |".format(generate_hyperlink(nar["name"], nar["html_url"]),
                                                                nar["description"],
                                                                nar["language"],
                                                                str(nar["stargazers_count"]),
                                                                nar["license"]["name"] if nar["license"] else "N/A") + \
                            "\n"
                text += "\n"

            if high_rated_repos:
                text += self.old_is_gold_blob
                for hrr in high_rated_repos:
                    text += "| {} | {} | {} | {} | {} |".format(generate_hyperlink(hrr["name"], hrr["html_url"]),
                                                                hrr["description"],
                                                                hrr["language"],
                                                                str(hrr["stargazers_count"]),
                                                                hrr["license"]["name"] if hrr["license"] else "N/A") + \
                            "\n"
            text = text.replace("None", "N/A")
            outfile.write(text)


class Fetcher:
    def __init__(self):

        self.API_BASE_URL = "https://api.github.com"
        self.API_TOKEN = "a90d00174ace2e47e4cbafe490610689a98903a0"
        self.WATCH_LIST_FNAME = "watch.list"
        self.TOPICS_LIST_FNAME = "topics.list"
        self.TASKS_LIST_FNAME = "tasks.list"
        self.ORGS_ENDPOINT = "/orgs/"
        self.DATE_SIX_MONTHS_AGO = datetime.now() - timedelta(days=180)

        with open(self.WATCH_LIST_FNAME, "r") as infile:
            self.watch_list = [line.strip("\n") for line in infile]

        with open(self.TOPICS_LIST_FNAME, "r") as infile:
            self.topics_list = set([line.strip("\n") for line in infile])

        with open(self.TASKS_LIST_FNAME, "r") as infile:
            self.tasks_list = set([line.strip("\n") for line in infile])

        self.writer = Writer()

    def get_repo_data(self, org):

        page = 1
        per_page = 100
        repo_data = []
        try:
            with requests.session() as sess:
                sess.headers.update({"Authorization": "token %s" % self.API_TOKEN})
                sess.headers.update({"Accept": "application/vnd.github.mercy-preview+json"})
                while True:
                    url = self.API_BASE_URL + self.ORGS_ENDPOINT + \
                          org + "/repos?page=" + \
                          str(page) + "&per_page=" + \
                          str(per_page)
                    resp = sess.get(url)
                    resp = json.loads(resp.text)
                    if not resp:
                        break
                    repo_data.extend(resp)
                    page += 1
                    if len(resp) < per_page:
                        break
        except Exception as err:
            return True, err
        else:
            return False, repo_data

    def filter_repos_based_on_topics(self, repo_data):
        filtered = []
        for repo in repo_data:
            if repo["topics"]:
                if len(set(repo["topics"]) & self.topics_list) >= 2:
                    filtered.append(repo)
                    continue
            if repo["description"]:
                unigrams = ngrams(repo["description"], 1)
                bigrams = ngrams(repo["description"], 2)
                bigram_dash = [bg.replace(" ", "-") for bg in bigrams]
                trigrams = ngrams(repo["description"], 3)
                quadgrams = ngrams(repo["description"], 4)
                if len(set(unigrams) & self.tasks_list) >= 1 or len(set(bigrams) & self.tasks_list) >= 1 or \
                        len(set(trigrams) & self.tasks_list) >= 1 or len(set(quadgrams) & self.tasks_list) >= 1 or \
                        len(set(bigram_dash) & self.topics_list) >= 1:
                    filtered.append(repo)
        return filtered

    def filter_repos_based_on_reputation(self, repo_data, very_less=False):

        star_gazers = list(map(lambda repo: repo["stargazers_count"], repo_data))

        if not very_less:
            avg_stars = int(sum(star_gazers) / len(star_gazers))
            high_rated_repos = list(filter(lambda repo: repo["stargazers_count"] > avg_stars, repo_data))
        else:
            high_rated_repos = repo_data

        newly_added_repos = list(filter(lambda repo: parser.parse(repo["created_at"])
                                        .replace(tzinfo=None) > self.DATE_SIX_MONTHS_AGO, repo_data))

        if newly_added_repos:
            overlapped_repos = set(list(map(lambda repo: repo["id"], high_rated_repos))) & \
                               set(list(map(lambda repo: repo["id"], newly_added_repos)))

            high_rated_repos = list(filter(lambda repo: repo["id"] not in overlapped_repos, high_rated_repos))

        high_rated_repos = sorted(high_rated_repos, key=lambda repo: repo["stargazers_count"], reverse=True)
        newly_added_repos = sorted(newly_added_repos, key=lambda repo: repo["stargazers_count"], reverse=True)

        return high_rated_repos, newly_added_repos

    def fetch_data(self):

        for org in self.watch_list:
            org_name, url = org.split(",")
            org_id = url.split("/")[-1]
            err, data = self.get_repo_data(org_id)
            if not err:
                filtered_by_topics = self.filter_repos_based_on_topics(data)

                if len(filtered_by_topics) < 20:
                    very_less = True
                else:
                    very_less = False

                high_rated_repos, newly_added_repos = self.filter_repos_based_on_reputation(filtered_by_topics,
                                                                                            very_less=very_less)

                if len(high_rated_repos) + len(newly_added_repos) > 0:
                    self.writer.write_information(org_name,
                                                  high_rated_repos,
                                                  newly_added_repos,
                                                  len(data),
                                                  url)
                print("Fetched {} repos from {} and wrote info for {} repos".format(len(data), url,
                                                                                    len(high_rated_repos) +
                                                                                    len(newly_added_repos)))
            else:
                print("Error {} in fetching repo data for {} : {}".format(data, org_name, url))


if __name__ == "__main__":
    fetcher = Fetcher()
    fetcher.fetch_data()
