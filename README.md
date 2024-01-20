# rizz

rizz is an automatic migration generator and query builder for sqlite for rust. Don't call it an orm.

*May or may not be inspired by [drizzle](https://github.com/drizzle-team/drizzle-orm)*

# Install

```sh
cargo add rizz_db
```

# Declare your schema

```rust
use rizz::*;
use serde::Deserialize;

#[table]
struct Posts {
  #[rizz(primary_key)]
  id: Integer,

  #[rizz(not_null)]
  body: Text
}

#[table]
struct Comments {
    #[rizz(primary_key)]
    id: Integer,

    #[rizz(not_null)]
    body: Text,

    #[rizz(references = "posts(id)")]
    post_id: Integer,
}

#[database("db.sqlite3")]
struct Database {
    posts: Posts,
    comments: Comments
}

#[row]
struct Post {
  id: u64,
  body: String
}

#[row]
struct Comment {
    id: u64,
    body: String,
    post_id: u64,
    post_body: Option<String>,
}

# Connect to your db

#[tokio::main]
async fn main() -> Result<(), rizz::Error> {
    // automatically migrates tables and columns
    let db = Database::new().await?;

    let Database { posts, comments } = &db;

    // add indices like this
    // technically if you hate auto migrations
    // you can use this to do them for create/drop table and add/drop column manually
    db.create(index("posts_id_body_idx").on(posts, (posts.id, posts.body)).unique().collate("nocase").desc().r#where("posts.body is not null"))
        .create(index("comments_body_idx").on(comments, (comments.post_id)))
        .migrate()
        .await?;

    // Inserting, updating, and deleting rows

    // insert into posts (id, body) values (?, ?) returning *
    let inserted_post: Post = db
        .insert(posts)
        .values(Post {
            id: 1,
            body: "".into(),
        })?
        .returning()
        .await?;

    // update posts set body = ?, id = ? where id = ?
    let rows_affected = db
        .update(posts)
        .set(Post {
            body: "post".into(),
            ..inserted_post
        })?
        .r#where(eq(posts.id, 1))
        .rows_affected()
        .await?;

    // delete from posts where id = ? returning *
    let deleted_post = db.delete(posts).r#where(eq(posts.id, 1)).returning().await;

    // querying

    // select * from comments
    let rows: Vec<Comment> = db.select(()).from(comments).all().await;

    // select id, body from comments
    let rows: Vec<Comment> = db.select((comments.id, comments.body)).from(comments).all().await;

    // select * from comments inner join posts on posts.id = comments.post_id
    let rows: Vec<Comment> = db
        .select(())
        .from(comments)
        .inner_join(posts, on(posts.id, comments.post_id))
        .all()
        .await;

    // prepared statements

    // select * from comments
    let query = db.select(()).from(comments);

    // prepare the query store it in a OnceLock<T> or something
    let prepared = query.prepare_as::<Comment>();

    // execute the prepared query later
    let rows = prepared.all().await?;

    Ok(())
}
```
