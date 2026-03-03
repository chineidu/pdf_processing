from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from src import ROOT, create_logger

# BASE_DIR = Path(__file__).resolve().parents[3]
logger = create_logger(name=__name__)
TEMPLATES_DIR = ROOT / "templates"

templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

router = APIRouter(tags=["ui"])


@router.get("/ui/auth", response_class=HTMLResponse)
async def auth_page(request: Request) -> HTMLResponse:
    """Render the authentication page with login and signup tabs."""
    logger.info("Rendering auth page")
    return templates.TemplateResponse("auth.html", {"request": request})


@router.get("/ui/upload", response_class=HTMLResponse)
async def upload_page(request: Request) -> HTMLResponse:
    """Render the upload page for one-click presigned URL and file upload workflow."""
    logger.info("Rendering upload page")
    return templates.TemplateResponse("upload.html", {"request": request})
