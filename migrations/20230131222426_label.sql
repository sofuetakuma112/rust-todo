CREATE TABLE labels (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL
);

-- DEFERRABLE INITIALLY DEFERREDによってトランザクションがコミットされるまでチェックを延期する
-- これがないとTodo追加時のトランザクション内でまだ実在しないTodoのidを参照していることによりエラーとなってしまう
CREATE TABLE todo_labels (
    id SERIAL PRIMARY KEY,
    DEFERRABLE INITIALLY DEFERRED todo_id INTEGER NOT NULL REFERENCES todos (id),
    label_id INTEGER NOT NULL REFERENCES labels (id) DEFERRABLE INITIALLY DEFERRED
);