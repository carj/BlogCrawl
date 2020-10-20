import time

from tinydb import TinyDB, Query

from pyPreservica import *
from bs4 import BeautifulSoup

workflow = WorkflowAPI(use_shared_secret=True)
client = EntityAPI(use_shared_secret=True)

parent_folder = "aba60557-6be6-49a7-ad02-ef385800d09b"
security_tag = "open"

workflow_contexts = workflow.get_workflow_contexts("com.preservica.core.workflow.web.crawl.and.ingest")
workflow_context = workflow_contexts.pop()


db = TinyDB("progress-db.json")

for n in range(1, 5, 1):
    URL = f"https://www.schroders.com/en/insights/index/{n}/"
    request = requests.get(URL)
    if request.status_code == requests.codes.ok:
        print(f"Opening URL {URL}")
        soup = BeautifulSoup(request.content, 'html.parser')
        for insight in soup.find_all("div", class_="tile single insight"):

            ## extract the blog URL
            seed_url = insight.parent["href"]
            print("Seed: " + seed_url)
            title = insight.find(class_="image").attrs["alt"]
            ## extract the blog Title
            print("Title: " + title)

            ## extract the blog date
            date =  insight.find(class_="date")
            year = date.string.split(" ")[2]
            month = date.string.split(" ")[1]
            print("Date: " + year + " : " + month)


            # check for already completed URLS
            query = Query()
            result = db.search(query.url == seed_url)
            if len(result) > 0:
                print(f"Already processed. Skipping...")
                continue

            folder = None
            year_folder = None

            tag = f"{year} {month}"

            ## does the year/month folder exist
            entities = client.identifier("insight-blog", tag)
            if len(entities) == 1:
                folder = entities.pop()
                folder = client.folder(folder.reference)

            ## check the parent year folder
            if folder is None:
                entities = client.identifier("insight-blog", year)
                if len(entities) == 0:
                    year_folder = client.create_folder(year, year, security_tag, parent_folder)
                    if year_folder:
                        client.add_identifier(year_folder, "insight-blog", year)
                if len(entities) == 1:
                    year_folder = entities.pop()
                    year_folder = client.folder(year_folder.reference)

                if year_folder is not None:
                    folder = client.create_folder(month, month, security_tag, year_folder.reference)
                    if folder:
                        client.add_identifier(folder, "insight-blog", tag)


            if folder:
                ## start the web crawl workflow with the URL
                workflow.start_workflow_instance(workflow_context, seedUrl=seed_url, itemTitle=title, SORef=folder.reference)

                # save the blog article which has been started.
                db_data = {"url": seed_url, "page": URL, "year": year, "month": month, "title": title}
                db.insert(db_data)
                time.sleep(60*5)



