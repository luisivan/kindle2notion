from notion.client import NotionClient
from notion.collection import NotionDate
from notion.block import QuoteBlock, TextBlock, PageBlock

from datetime import datetime
from clippings.parser import parse_clippings
import concurrent.futures
import functools
from settings import CLIPPINGS_FILE, NOTION_TOKEN, NOTION_TABLE_ID

class KindleClippings(object):
    def __init__(self, clippingsFile):
        clippingsFile = open(CLIPPINGS_FILE, 'r', encoding="utf-8-sig")
        parsedClippings = parse_clippings(clippingsFile)
        print("Found", len(parsedClippings), "notes and highlights" )

        self.docs = {}
        self.clippings = {}

        for clipping in parsedClippings:
            if clipping.document.title not in self.clippings.keys():
                self.docs[clipping.document.title] = clipping.document
                self.clippings[clipping.document.title] = []

            self.clippings[clipping.document.title].append(clipping.to_dict())

        self._addMissingRows(self.clippings.keys(), self.docs)
        self._addMissingClippingsToRow(self.clippings)

    def _addMissingRows(self, allTitles, allDocs):
        global allRows
        syncedTitles = []  
        for eachRow in allRows:
            syncedTitles.append(eachRow.title)

        s = set(syncedTitles)
        missingTitles = [x for x in allTitles if x not in s]

        for missingTitle in missingTitles:
            print("Adding missing title", missingTitle)
            row = cv.collection.add_row()
            row.title = allDocs[missingTitle].title
            row.author = allDocs[missingTitle].authors
            row.highlights = 0

    def _addMissingClippingsToRow(self, clippings):
        global allRows

        for row in allRows:
            if row.title in self.clippings.keys():
                partialAddClip = functools.partial(self._addClipToRow, row)
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    updated = list(executor.map(partialAddClip, self.clippings[row.title]))
                    if True in updated:
                        row.last_synced = NotionDate(datetime.now())
                    print('Finished updating', row.title)

    def _addClipToRow(self, row, clipping):
        if row.last_synced and row.last_synced.start > clipping['metadata']['timestamp']:
            return False

        clipExists = False

        parentPage = client.get_block(row.id)
        allClippings = parentPage.children.filter(QuoteBlock)
        for eachClip in allClippings:
            if clipping['content'].strip() == eachClip.title:
                clipExists = True
        if clipExists == False:
            print("Adding a new highlight to", row.title)
            parentPage.children.add_new(
                QuoteBlock,
                title = clipping['content']
            )
            row.highlights +=1
            row.last_highlighted = NotionDate(clipping['metadata']['timestamp'])
        
        return True

client = NotionClient(token_v2= NOTION_TOKEN)
cv = client.get_collection_view(NOTION_TABLE_ID)
allRows = cv.collection.get_rows()
print(cv.parent.views)

ch = KindleClippings(CLIPPINGS_FILE)