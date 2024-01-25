# ryzz

ryzz is an automatic migration generator and query builder for sqlite in rust. Don't call it an orm.

# Install

```sh
cargo add ryzz
```

# Declare your schema

```rust
use ryzz::*;

#[database]
struct Database {
    posts: Posts,
    comments: Comments
}

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

    #[rizz(references = "Posts(id)")]
    post_id: Integer,
}

// TODO stick tables and columns in a OnceLock vec at compile time
// TODO check those tables and columns from vec from the row proc_macro_attribute

#[row(Posts)] // (Posts) is optional but it double checks that your types match up at compile time
struct Post {
  id: u64,
  body: String
}

#[row(Comments)]
struct Comment {
    id: u64,
    body: String,
    post_id: u64,
}
```

# Insert, update and delete

```rust
// automatically migrates tables and columns
let db = Database::new("db.sqlite3").await?;

let Database { posts, comments } = &db;

// insert into posts (id, body) values (?, ?) returning *
let inserted: Post = db
    .insert_into(posts)
    .values(Post {
        id: 1,
        body: "".into(),
    })?
    .returning()
    .await?;

// update posts set body = ?, id = ? where id = ? returning *
let updated: Post = db
    .update(posts)
    .set(Post {
        body: "post".into(),
        ..inserted
    })?
    .r#where(eq(posts.id, 1))
    .returning()
    .await?;

// delete from posts where id = ? returning *
let deleted: Post = db.delete_from(posts).r#where(eq(posts.id, 1)).returning().await?;
```

# Querying

```rust
// select ... from comments
let rows: Vec<Comment> = db.select(()).from(comments).all().await?;

// select ... from comments
let rows: Vec<Comment> = db.select((comments.id, comments.body)).from(comments).all().await?;

#[row]
struct CommentWithPost {
    comment: Comment,
    post: Post
}

// select ... from comments inner join posts on posts.id = comments.post_id
let rows = db
    .select(())
    .from(comments)
    .inner_join(posts, eq(posts.id, comments.post_id))
    .all::<CommentWithPost>()
    .await?;
```

# Prepared Statements

```rust
let query = db.select(()).from(comments);

let prepared = query.prepare::<Comment>();

let rows: Vec<Comment> = prepared.all().await?;
```

# Manage Indexes/Indices (same thing)

```rust
let ix = index("posts_id_body_ix").unique().on(posts, (posts.id, posts.body));

// create unique index if not exists posts_id_body_ix on posts (id, body);
db.create(&ix).await?;

// drop index if exists posts_id_body_ix;
db.drop(&ix).await?;
```

# Easier insert, update and delete

```rust
// insert into posts (id, body) values (?, ?) returning *
let mut post: Post = db.insert(Post { id: 1, body: "".into() }).await?;

// update posts set body = ? where id = ? returning *
let post: Post = db.update_row(Post { body: "update".into(), ..post }).await?;

// delete from posts where id = ? returning *
let post: Post = db.delete(post).await?;
```

# Supported types

| Sqlite | Rust |
| ------------- | ------------- |
| Text | String |
| Integer | i64 |
| Real | f64 |
| Null | None |
| Blob | Vec<u8> |
