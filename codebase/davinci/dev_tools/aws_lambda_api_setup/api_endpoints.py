from fastapi import FastAPI
from mangum import Mangum
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title='DaVinci API Endpoints',
              description='APIs to interact with predictive models')

origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    # allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def read_root():
    return {"API": "You're at the root. Use http://127.0.0.1:8000/docs to test API endpoints."}


# to make it work with Amazon Lambda, we create a handler object
handler = Mangum(app=app)
