# rizz_db

rizz_db is an automatic migration generator and query builder for sqlite in rust. Don't call it an orm.

# Install

```sh
cargo add rizz_db
```

# Declare your schema

```rust
use rizz_db::*;

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
```

# Insert, update and delete

```rust
// automatically migrates tables and columns
let db = Database::new("db.sqlite3").await?;

let Database { posts, comments } = &db;

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
let deleted_post: Post = db.delete_from(posts).r#where(eq(posts.id, 1)).returning().await?;
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
let rows: Vec<CommentWithPost> = db
    .select(())
    .from(comments)
    .inner_join(posts, on(posts.id, comments.post_id))
    .all()
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
