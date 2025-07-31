from fastapi import FastAPI, Depends, Request
import actions

app = FastAPI()

actions_obj = actions.actions()

@app.post("/chunking")
async def chunking(file_path: str):
    try:
        return await actions_obj.chunking(file_path)
    except Exception as e:
        return {"error": str(e)}
    
@app.post("/merging")
def merging(root_folder: str):
    try:
        return actions_obj.merging(root_folder)
    except FileNotFoundError as e:
        return {"error": str(e)}
    except Exception as e:
        return {"error": "An unexpected error occurred: " + str(e)}
    