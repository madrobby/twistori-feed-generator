import {
  OutputSchema as RepoEvent,
  isCommit,
} from './lexicon/types/com/atproto/sync/subscribeRepos'
import { FirehoseSubscriptionBase, getOpsByType } from './util/subscription'

export class FirehoseSubscription extends FirehoseSubscriptionBase {
  public postCount = 0
  public lastSkeet = Date.now()
  public timestamps: number[] = []
  public counts: number[] = []

  async handleEvent(evt: RepoEvent) {
    if (!isCommit(evt)) return

    const ops = await getOpsByType(evt)
    const STOPWORDS = /\b(musk|elon|muskrat|#[a-z0-9]+|bluesky|twistori|jay|paul|@[a-z0-9]|bsky)\b/gi
    const PROGRESS = ['|','/','-','\\']

    const now = Date.now()
    const depth = 500

    this.timestamps.push(now - this.lastSkeet)
    this.timestamps = this.timestamps.slice(-depth)
    this.counts.push(ops.posts.creates.length)
    this.counts = this.counts.slice(-depth)

    const avgTime = this.timestamps.reduce((a, b) => a + b, 0) / this.timestamps.length
    const avgSkeets = this.counts.reduce((a, b) => a + b, 0) / this.counts.length

    const sps = (1000.0 / avgTime) * avgSkeets
    this.lastSkeet = now

    const postsToDelete = ops.posts.deletes.map((del) => del.uri)
    const postsToCreate = ops.posts.creates
      .filter((create) => {
        const ltext = create.record.text.toLowerCase()

        const match =
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

        this.postCount = this.postCount + ops.posts.creates.length
        process.stdout.write("\r\u001b[2K"+PROGRESS[this.postCount % 4]+' ['+this.postCount+'] '+(Math.round(sps * 100) / 100).toFixed(2)+" skeets/s")

        if (match) {
          process.stdout.write('\n')
          console.log(create.record.text)
        }
        return match
      })
      .map((create) => {
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
