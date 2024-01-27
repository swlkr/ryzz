# ryzz

ryzz is an automatic migration generator and query builder for sqlite in rust.

- Automatic append only migrations
- Sql-like syntax
- Query builder not an orm

# Install

```sh
cargo add ryzz
```

# Declare your schema

```rust
use ryzz::*;

#[table("posts")]
struct Post {
    #[ryzz(pk)]
    id: i64,
    title: Option<String>,
    body: String
}

#[table]
struct Comment {
    #[ryzz(pk)]
    id: i64,
    body: String,
    #[ryzz(refs = "posts(id)")]
    post_id: i64,
}
```

# Insert, update and delete

```rust
let db = Database::new("db.sqlite3").await?;
let posts = Post::table(&db).await?;
let comments = Comment::table(&db).await?;

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
// select ... from Comment
let rows: Vec<Comment> = db.select(()).from(comments).all().await?;

// select ... from Comment
let rows: Vec<Comment> = db.select((comments.id, comments.body)).from(comments).all().await?;
```

# Joins

```rust
#[row]
struct CommentWithPost {
    comment: Comment,
    post: Post
}

// select ... from Comment inner join posts on posts.id = Comment.post_id
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

# Sqlite to rust type map

| Sqlite | Rust |
| ------------- | ------------- |
| Text | String |
| Integer | i64 |
| Real | f64 |
| Null | None |
| Blob | Vec&lt;u8&gt; |

# Automatic migrations

- Schema migrations only ever `create table` or `alter table add column`. Inspired by [trevyn/turbosql](https://github.com/trevyn/turbosql)
- When `<Your Table>::table(&db).await?` is called, the migrations are run.
