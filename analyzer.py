import re
import os
import json
import requests
from datetime import datetime, timedelta
from collections import Counter
from dateutil import parser
from config import Config
import plotly.graph_objects as go
import pandas as pd

cfg = Config()


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

        self.last_updated = "### Last Updated On:" + datetime.now().strftime("%d-%m-%y") + "\n\n"

        self.new_arrival_blob = "## Newly Added" + "\n\n" + "| Name | Description | Language | Stars | License |" + \
                                "\n" \
                                + "| ---- | ----------- | :--------: | :-----: | :-------: |" + "\n"

        self.old_is_gold_blob = "## Highly Rated" + "\n\n" + "| Name | Description | Language | Stars | License |" + \
                                "\n" + \
                                "| ---- | ----------- | :--------: | :-----: | :-------: |" + "\n "

    def write_information(self, org_name, high_rated_repos, newly_added_repos, no_of_total_repos, url):

        with open(os.path.join(self.data_directory_path, org_name.lower() + self.ext), "w") as outfile:
            text = self.header.replace("n_org", generate_hyperlink(org_name, url)). \
                replace("n_total_repos", str(no_of_total_repos)). \
                replace("n_repos", str(len(high_rated_repos) + len(newly_added_repos))). \
                replace("name_org", org_name)

            text += self.last_updated

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
        self.API_TOKEN = cfg.get_configs()["api_key"]
        self.WATCH_LIST_FNAME = "watch.list"
        self.TOPICS_LIST_FNAME = "topics.list"
        self.TASKS_LIST_FNAME = "tasks.list"
        self.ORGS_ENDPOINT = "/orgs/"
        self.DATE_SIX_MONTHS_AGO = datetime.now() - timedelta(days=180)
        self.data_dump_fname = "dump.json"

        with open(self.WATCH_LIST_FNAME, "r") as infile:
            self.watch_list = [line.strip("\n") for line in infile]

        with open(self.TOPICS_LIST_FNAME, "r") as infile:
            self.topics_list = set([line.strip("\n") for line in infile])

        with open(self.TASKS_LIST_FNAME, "r") as infile:
            self.tasks_list = set([line.strip("\n") for line in infile])

        self.writer = Writer()

    def dump_data_in_file(self, data):
        with open(self.data_dump_fname, "w") as infile:
            json.dump(data, infile)

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
        stats = []
        data_dump = []
        filtered_ids = []

        for org in self.watch_list:
            org_name, url = org.split(",")
            org_id = url.split("/")[-1]
            err, data = self.get_repo_data(org_id)
            if not err:
                forks_excluded = list(filter(lambda repo: not repo["fork"], data))
                filtered_by_topics = self.filter_repos_based_on_topics(forks_excluded)
                filtered_ids.extend(list(map(lambda repo: repo["id"], filtered_by_topics)))

                if len(filtered_by_topics) < 30:
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
                stats.append((org_name, len(high_rated_repos) + len(newly_added_repos)))
                data_dump.extend(data)
            else:
                print("Error {} in fetching repo data for {} : {}".format(data, org_name, url))

        filtered_ids = set(filtered_ids)

        for repo_d in data_dump:
            if repo_d["id"] in filtered_ids:
                repo_d["filtered"] = True
            else:
                repo_d["filtered"] = False

        self.dump_data_in_file(data_dump)

        return stats


class AnalysisPlotter:
    def __init__(self):

        self.data_dump_fname = "dump.json"
        self.repo_data = self.load_data()
        self.plots_dir = "plots/"

        with open("tasks.list", "r") as infile:
            self.tasks_list = set([line.strip("\n") for line in infile])

    def load_data(self):

        with open(self.data_dump_fname, "r") as infile:
            data = json.loads(infile.read())

        return pd.io.json.json_normalize(data)

    def bar_plot_template(
            self,
            x_axis,
            y_axis,
            plot_title,
            x_axis_title,
            y_axis_title,
            texttemplate="%{y}",
            textposition="outside",
            colorscale="Blugrn",
            showscale=False,
            width=None,
            height=None):

        fig = go.Figure(data=go.Bar(
            x=x_axis,
            y=y_axis,
            texttemplate=texttemplate,
            textposition=textposition,
            marker={"color": y_axis,
                    "colorscale": colorscale,
                    "showscale": showscale}
        ))

        fig.update_layout(
            title=plot_title,
            xaxis_title=x_axis_title,
            yaxis_title=y_axis_title,
            autosize=True
        )

        # fig.show()
        if width and height:
            fig.write_image(self.plots_dir + plot_title.replace(" ", "_").replace("/", "_").lower() + ".svg",
            width=width, height=height)
        else:
            fig.write_image(self.plots_dir + plot_title.replace(" ", "_").replace("/", "_").lower() + ".svg")

    def scatter_plot_template(
            self,
            x_axis,
            y_axis,
            plot_title,
            x_axis_title,
            y_axis_title,
            mode="lines+markers+text",
            texttemplate="%{y}",
            textposition="top right",
            colorscale="Blugrn",
            size=10,
            showscale=False,
            width=None,
            height=None):

        fig = go.Figure(data=go.Scatter(
            x=x_axis,
            y=y_axis,
            mode=mode,
            textposition=textposition,
            texttemplate=texttemplate,
            marker={
                "color": y_axis,
                "colorscale": colorscale,
                "size": size
            }
        ))

        fig.update_layout(
            title=plot_title,
            xaxis_title=x_axis_title,
            yaxis_title=y_axis_title,
            autosize=True
        )

        # fig.show()
        if width and height:
            fig.write_image(self.plots_dir + plot_title.replace(" ", "_").replace("/", "_").lower() + ".svg",
            width=width, height=height)
        else:
            fig.write_image(self.plots_dir + plot_title.replace(" ", "_").replace("/", "_").lower() + ".svg")

    def plot_all(self):

        filtered = self.repo_data[self.repo_data["filtered"] == True]
        unfiltered = self.repo_data[self.repo_data["filtered"] == False]

        filtered.loc[:, "created_at"] = filtered["created_at"].apply(pd.to_datetime).apply(
            lambda x: x.tz_localize(None))
        filtered.loc[:, "updated_at"] = filtered["updated_at"].apply(pd.to_datetime).apply(
            lambda x: x.tz_localize(None))

        # consider jupyter notebook as python
        filtered.loc[filtered["language"] == "Jupyter Notebook", "language"] = "Python"

        # Distribution of programming languages over all repos
        grpby_languages = filtered.groupby(["language"]).size()
        grpby_languages = grpby_languages[grpby_languages > 10].sort_values(ascending=False)

        self.bar_plot_template(
            grpby_languages.index,
            grpby_languages.values,
            "Distribution of Programming Languages",
            "Programming Languages",
            "Number of Repos",
            width=1024,
            height=768)

        # Distribution of stars
        bins = [i for i in range(0, 23000, 1000)] + [filtered["stargazers_count"].max() + 1000]
        binned = pd.cut(filtered["stargazers_count"], bins=bins).value_counts(sort=False)

        self.bar_plot_template(
            [str(bins[i]) + "-" + str(bins[i + 1]) for i in range(len(bins) - 1)],
            binned.values,
            "Distribution of Stars",
            "Stars in range",
            "Number of Repos",
            width=1024,
            height=768)

        # Portion of DS/ML tool over all repositories
        grpby_org_not_filtered = unfiltered.groupby("owner.login").size()
        grpby_org_all = self.repo_data.groupby("owner.login").size()
        portion = ((1 - (grpby_org_not_filtered / grpby_org_all)) * 100).sort_values(ascending=False)
        portion = portion[portion > 5.0].astype(int)

        self.scatter_plot_template(
            portion.index,
            portion.values,
            "Portion of Data Science / Machine Learning tools over all repos",
            "Organizations",
            "Percentage value",
            texttemplate="%{y}%",
            textposition="top right",
            width=1024,
            height=768
        )

        # Repositories created over time
        grpby_creation_year = filtered.groupby(filtered.created_at.dt.year).size()

        self.bar_plot_template(
            grpby_creation_year.index,
            grpby_creation_year.values,
            "Repositories created over time",
            "Creation Year",
            "Number of Repos"
        )

        # Repositories updated over time
        labels = ["Last 15 days", "Last Month", "Last Six Months"]
        y_axis = [filtered[filtered["updated_at"] > (datetime.now() - timedelta(days=i))].shape[0] for i in
                  [15, 30, 180]]
        y_axis = [int(val / filtered.shape[0] * 100) for val in y_axis]

        self.bar_plot_template(
            labels,
            y_axis,
            "Repositories Updated over time",
            "Timespan",
            "Percentage of Repos",
            texttemplate="%{y}%"
        )

        # Common Github ML_DS tasks
        common_tasks = []
        tobe_removed = []
        for des in filtered["description"].values:
            if des is not None:
                unigrams = ngrams(des, 1)
                bigrams = ngrams(des, 2)
                bigram_dash = [bg.replace(" ", "-") for bg in bigrams]
                trigrams = ngrams(des, 3)
                quadgrams = ngrams(des, 4)
                if len(set(unigrams) & self.tasks_list) >= 1:
                    common_tasks.extend(list(set(unigrams) & self.tasks_list))
                if len(set(bigrams) & self.tasks_list) >= 1:
                    common_tasks.extend(list(set(bigrams) & self.tasks_list))
                if len(set(trigrams) & self.tasks_list) >= 1:
                    common_tasks.extend(list(set(trigrams) & self.tasks_list))
                if len(set(quadgrams) & self.tasks_list) >= 1:
                    common_tasks.extend(list(set(quadgrams) & self.tasks_list))

        common_tasks = Counter(common_tasks)
        for k in common_tasks.keys():
            for j in common_tasks.keys():
                if k != j and k in j:
                    common_tasks[j] += common_tasks[k]
                    tobe_removed.append(k)

        if tobe_removed:
            for tr in tobe_removed:
                del common_tasks[tr]

        common_tasks = common_tasks.most_common(30)

        self.scatter_plot_template(
            list(map(lambda x: x[0], common_tasks)),
            list(map(lambda x: x[1], common_tasks)),
            "Common Data Science Tasks",
            "Tasks",
            "Number of Repos",
            width=1024,
            height=768
        )

if __name__ == "__main__":
    fetcher = Fetcher()
    stats = fetcher.fetch_data()
    analysis_plotter = AnalysisPlotter()
    analysis_plotter.plot_all()
