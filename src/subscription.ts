import {
  OutputSchema as RepoEvent,
  isCommit,
} from './lexicon/types/com/atproto/sync/subscribeRepos'
import { FirehoseSubscriptionBase, getOpsByType } from './util/subscription'

export class FirehoseSubscription extends FirehoseSubscriptionBase {
  public postCount = 0
  public lastSkeet = Date.now()

  async handleEvent(evt: RepoEvent) {
    if (!isCommit(evt)) return
    const ops = await getOpsByType(evt)
    const STOPWORDS = /\b(musk|elon|muskrat|#[a-z0-9]+|bluesky|twistori|jay|paul|@[a-z0-9]|bsky)\b/gi
    // This logs the text of every post off the firehose.
    // Just for fun :)
    // Delete before actually using
    for (const post of ops.posts.creates) {
//      console.log("--> "+JSON.stringify(post.record))
      // console.log(post.record.text)
    }

    const now = Date.now()
    const sps = (1000.0/(now - this.lastSkeet)) * ops.posts.creates.length
    this.lastSkeet = now

    const postsToDelete = ops.posts.deletes.map((del) => del.uri)
    const postsToCreate = ops.posts.creates
      .filter((create) => {
        const ltext = create.record.text.toLowerCase()

        const match =
          // todo: filter stopwords
          // todo: filter hashtags (usually sports or nsfw)
          // todo: filter longer entries?
          create.record.facets === undefined &&
          create.record.reply === undefined &&
          (!ltext.match(STOPWORDS)) &&
          (
            ltext.match(/\bi\slove\b/) ||
            ltext.match(/\bi\shate\b/) ||
            ltext.match(/\bi\sthink\b/) ||
            ltext.match(/\bi\sbelieve\b/) ||
            ltext.match(/\bi\sfeel\b/) ||
            ltext.match(/\bi\swish\b/)
          )

          // todo: for generating feed files
          // take last 50 of every category and randomize
          // update once an hour maybe?
        // print console.log('\033[2J');
        this.postCount = this.postCount + 1
        process.stdout.write("\u001b[2K\u001b[0E --> post "+this.postCount+" "+sps+" skeets/s")

        if (match) {
          process.stdout.write('\n')
          console.log(create.record.text)
        }
        return match
      })
      .map((create) => {
        // map alf-related posts to a db row
        return {
          uri: create.uri,
          cid: create.cid,
          replyParent: create.record?.reply?.parent.uri ?? null,
          replyRoot: create.record?.reply?.root.uri ?? null,
          indexedAt: new Date().toISOString(),
          text: create.record?.text
        }
      })

    if (postsToDelete.length > 0) {
      await this.db
        .deleteFrom('post')
        .where('uri', 'in', postsToDelete)
        .execute()
    }
    if (postsToCreate.length > 0) {
      await this.db
        .insertInto('post')
        .values(postsToCreate)
        .onConflict((oc) => oc.doNothing())
        .execute()
    }
  }
}
