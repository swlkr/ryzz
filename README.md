# ryzz

ryzz is an automatic migration generator and query builder for sqlite in rust.

- Schema is auto migrated by `#[table]` structs
- Tries to stick to sql-like syntax
- Query builder not an orm

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
  id: Pk<Integer>,
  title: Null<Text>,
  body: Text
}

#[table]
struct Comments {
    id: Pk<Integer>,
    body: Text,
    #[ryzz(references = "Posts(id)")]
    post_id: Integer,
}

 // (Posts) is optional; checks that your row types match the table types
#[row(Posts)]
struct Post {
  id: i64,
  title: Option<String>,
  body: String
}

#[row(Comments)]
struct Comment {
    id: i64,
    body: String,
    post_id: i64,
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
        title: None,
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
```

# Joins

```rust
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

# Manage Indices

```rust
let ix = index("posts_id_body_ix").unique().on(posts, (posts.id, posts.body));

// create unique index if not exists posts_id_body_ix on posts (id, body);
db.create(&ix).await?;

// drop index if exists posts_id_body_ix;
db.drop(&ix).await?;
```

# Supported types

| Sqlite | Rust |
| ------------- | ------------- |
| Text | String |
| Integer | i64 |
| Real | f64 |
| Null | None |
| Blob | Vec&lt;u8&gt; |

# Auto schema migrations

On compile, the `#[table]` macro runs `create table if not exists` or `alter table add column` for any new structs or struct fields ryzz hasn't seen before.

- Schema migrations are one-way, append-only. Inspired by [trevyn/turbosql](https://github.com/trevyn/turbosql)
- When `Database::new()` is called, the append migrations are run
