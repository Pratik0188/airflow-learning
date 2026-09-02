from fastapi import FastAPI, HTTPException, status
import uvicorn

app = FastAPI()

data = [
    {"id": 1, "name": "Alice", "age": 30},
    {"id": 2, "name": "Bob", "age": 25},
    {"id": 3, "name": "Charlie", "age": 35},
]


@app.get("/")
def read_root():
    return {"message": "Welcome to the Users API. Visit /docs for interactive documentation."}


@app.get("/users")
def get_users():
    return data


@app.get("/users/{user_id}")
def get_user(user_id: int):
    for user in data:
        if user["id"] == user_id:
            return user
    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")


@app.post("/users", status_code=status.HTTP_201_CREATED)
def create_user(user: dict):
    if any(u["id"] == user.get("id") for u in data):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="ID already exists")
    data.append(user)
    return user


@app.put("/users/{user_id}")
def update_user(user_id: int, updated_user: dict):
    for i, user in enumerate(data):
        if user["id"] == user_id:
            data[i] = {**user, **updated_user, "id": user_id}
            return data[i]
    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")


@app.delete("/users/{user_id}")
def delete_user(user_id: int):
    for i, user in enumerate(data):
        if user["id"] == user_id:
            return data.pop(i)
    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)