from tinydb import TinyDB, Query
from pyPreservica import *
from bs4 import BeautifulSoup
import xml.etree.ElementTree

workflow = WorkflowAPI(use_shared_secret=True)
client = EntityAPI(use_shared_secret=True)
content = ContentAPI(use_shared_secret=True)

parent_folder = "aba60557-6be6-49a7-ad02-ef385800d09b"
security_tag = "open"

workflow_contexts = workflow.get_workflow_contexts("com.preservica.core.workflow.web.crawl.and.ingest")
workflow_context = workflow_contexts.pop()

db = TinyDB("progress-db.json")

for n in range(38, 39, 1):
    URL = f"https://www.schroders.com/en/insights/index/{n}/"
    request = requests.get(URL)
    if request.status_code == requests.codes.ok:
        print(f"Opening URL {URL}")
        soup = BeautifulSoup(request.content, 'html.parser')
        for insight in soup.find_all("div", class_="tile single insight"):

            # extract the blog URL
            seed_url = insight.parent["href"]
            print("Seed URL: " + seed_url)

            # check for already completed URLS
            query = Query()
            if db.contains(query.url == seed_url):
                print(f"Already processed. Skipping...")
                continue

            title = insight.find(class_="image").attrs["alt"]
            # extract the blog Title
            print("Title: " + title)

            category = ""
            category_container = insight.find(class_="category")
            if category_container is not None:
                category = category_container.string
                print(category)

            # extract the blog date
            date = insight.find(class_="date")
            year = date.string.split(" ")[2]
            month = date.string.split(" ")[1]
            print("Date: " + year + " : " + month)

            article_description = ""
            creationdate = ""
            publicationdate = ""
            section = ""
            assetclass = ""

            author_list = list()
            try:
                request = requests.get(seed_url)
                if request.status_code == requests.codes.ok:
                    url_soup = BeautifulSoup(request.content, 'html.parser')
                    author_profiles = url_soup.find("div", class_="author-profiles")
                    if author_profiles is not None:
                        author_profiles = author_profiles.find_all("div", class_="author")
                        for author in author_profiles:
                            author_list.append(
                                (author.find("p", class_="name").string, author.find("p", class_="title").string))
                    article_description = url_soup.find("meta", {"name": "description"})["content"]
                    creationdate = url_soup.find("meta", {"name": "creationdate"})["content"]
                    publicationdate = url_soup.find("meta", {"name": "publicationdate"})["content"]
                    section = url_soup.find("meta", {"name": "section"})["content"]
                    assetclass = url_soup.find("meta", {"name": "assetclass"})["content"]
            except:
                pass

            folder = None
            year_folder = None

            tag = f"{year} {month}"

            # does the year/month folder exist
            entities = client.identifier("insight-blog", tag)
            if len(entities) == 1:
                folder = entities.pop()
                folder = client.folder(folder.reference)

            # check the parent year folder
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
                # start the web crawl workflow with the URL
                pid = workflow.start_workflow_instance(workflow_context, seedUrl=seed_url, itemTitle=title,
                                                       SORef=folder.reference)
                # save the blog article which has been started.
                db_data = {"url": seed_url, "page": URL, "year": year, "month": month, "title": title}
                db.insert(db_data)

                # wait for crawl to finish
                sleep_sec = 1
                while True:
                    status = client.get_async_progress(pid)
                    if status != "ACTIVE":
                        print("")
                        print("Ingest Complete")
                        break
                    else:
                        sys.stdout.write("%s" % ".")
                        sys.stdout.flush()
                        time.sleep(sleep_sec)
                        sleep_sec = sleep_sec + 1

                print("\n")
                search_url = seed_url.lstrip("https://")
                map_fields = {"xip.reference": "*", "xip.description": search_url,
                              "xip.document_type": "IO", "xip.parent_ref": folder.reference}
                for result in content.search_index_filter_list("%", 1, map_fields):
                    if result["xip.description"] == seed_url:
                        print("Found Asset. Updating Metadata....")
                        asset = client.asset(result["xip.reference"])
                        db.upsert({'ingested': True, "asset ref": asset.reference}, query.url == seed_url)
                        asset.title = title
                        client.save(asset)

                        xml_object = xml.etree.ElementTree.Element('Insights',
                                                                   {"xmlns": "https://www.schroders.com/en/insights/"})
                        xml.etree.ElementTree.SubElement(xml_object, "Category").text = category
                        for a in author_list:
                            author_object = xml.etree.ElementTree.SubElement(xml_object, "Author")
                            xml.etree.ElementTree.SubElement(author_object, "Name").text = a[0]
                            xml.etree.ElementTree.SubElement(author_object, "Title").text = a[1]
                        xml.etree.ElementTree.SubElement(xml_object, "Description").text = article_description
                        xml.etree.ElementTree.SubElement(xml_object, "CreationDate").text = creationdate.replace(" ",
                                                                                                                 "T")
                        xml.etree.ElementTree.SubElement(xml_object, "PublicationDate").text = publicationdate.replace(
                            " ", "T")
                        xml.etree.ElementTree.SubElement(xml_object, "URL").text = seed_url
                        xml.etree.ElementTree.SubElement(xml_object, "Section").text = section
                        xml_request = xml.etree.ElementTree.tostring(xml_object, encoding='utf-8', xml_declaration=True)
                        client.add_metadata(asset, "https://www.schroders.com/en/insights/",
                                            xml_request.decode("utf-8"))
