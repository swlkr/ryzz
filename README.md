# rizz

rizz is an automatic migration generator and query builder for sqlite for rust. Don't call it an orm.

*May or may not be inspired by [drizzle](https://github.com/drizzle-team/drizzle-orm)*

# Install

```sh
cargo add rizz_db # rizz was already taken on crates.io :(
```

# Declare your schema

```rust
use rizz::*;
use serde::Deserialize;

#[database]
struct Database {
    posts: Posts,
    comments: Comments
}

#[table("posts")]
struct Posts {
  #[rizz(primary_key)]
  id: Integer,

  #[rizz(not_null)]
  body: Text
}

#[table("comments")]
struct Comments {
    #[rizz(primary_key)]
    id: Integer,

    #[rizz(not_null)]
    body: Text,

    #[rizz(references = "posts(id)")]
    post_id: Integer,
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
    #[serde(default)]
    post_id: u64,
}

# Connect to your db

#[tokio::main]
async fn main() -> Result<(), rizz::Error> {
    // automatically migrates tables and columns
    let db = Database::new("db.sqlite3").await?;

    let Database { posts, comments, .. } = &db;

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

    // update posts set body = ?, id = ? where id = ? returning *
    let updated_post: Post = db
        .update(posts)
        .set(Post {
            body: "post".into(),
            ..inserted_post
        })?
        .r#where(eq(posts.id, 1))
        .returning()
        .await?;

    // delete from posts where id = ? returning *
    let deleted_post = db.delete(posts).r#where(eq(posts.id, 1)).returning().await?;

    // querying

    // select json_object('id', "comments"."id") from comments
    let rows: Vec<Comment> = db.select(()).from(comments).all().await?;

    // select id, body from comments
    let rows: Vec<Comment> = db.select((comments.id, comments.body)).from(comments).all().await?;

    #[row]
    struct CommentWithPost {
        comments: Comment,
        posts: Post
    }

    // select * from comments inner join posts on posts.id = comments.post_id
    let rows: Vec<CommentWithPost> = db
        .select(())
        .from(comments)
        .inner_join(posts, on(posts.id, comments.post_id))
        .all()
        .await?;

    // prepared statements

    // select * from comments
    let query = db.select(()).from(comments);

    // prepare the query
    let prepared = query.prepare_as::<Comment>();

    // execute the prepared query later
    let rows: Vec<Comment> = prepared.all().await?;

    Ok(())
}
```