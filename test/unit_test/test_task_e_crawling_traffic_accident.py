"""Unit tests for src.task.e_crawling_traffic_accident module."""

import pytest
from unittest.mock import patch, MagicMock
from pathlib import Path
import tempfile
import requests
from src.task.e_crawling_traffic_accident import (
    find_download_links,
    download_and_extract_zip
)


class TestFindDownloadLinks:
    """Test suite for find_download_links function."""

    @patch("src.task.e_crawling_traffic_accident.BeautifulSoup")
    @patch("src.task.e_crawling_traffic_accident.requests.get")
    def test_find_download_links_success(self, mock_get, mock_soup_class):
        """Test successful link finding."""
        # Set up mocks
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_get.return_value = mock_response
        
        mock_soup = MagicMock()
        mock_soup_class.return_value = mock_soup
        
        urls = ["https://data.gov.tw/dataset/177136"]
        headers = {"User-Agent": "test-agent"}
        
        result = find_download_links(urls, headers)
        
        assert isinstance(result, dict)
        assert mock_get.called

    @patch("src.task.e_crawling_traffic_accident.requests.get")
    def test_find_download_links_empty_urls(self, mock_get):
        """Test with empty URL list."""
        urls = []
        headers = {"User-Agent": "test-agent"}
        
        result = find_download_links(urls, headers)
        
        assert result == {}
        mock_get.assert_not_called()

    @patch("src.task.e_crawling_traffic_accident.requests.get")
    def test_find_download_links_timeout(self, mock_get):
        """Test timeout handling."""
        mock_get.side_effect = requests.exceptions.Timeout()
        
        urls = ["https://data.gov.tw/dataset/177136"]
        headers = {"User-Agent": "test-agent"}
        
        result = find_download_links(urls, headers)
        
        assert isinstance(result, dict)

    @patch("src.task.e_crawling_traffic_accident.requests.get")
    def test_find_download_links_connection_error(self, mock_get):
        """Test connection error handling."""
        mock_get.side_effect = requests.exceptions.ConnectionError()
        
        urls = ["https://data.gov.tw/dataset/177136"]
        headers = {"User-Agent": "test-agent"}
        
        result = find_download_links(urls, headers)
        
        assert isinstance(result, dict)

    @patch("src.task.e_crawling_traffic_accident.requests.get")
    def test_find_download_links_bad_status(self, mock_get):
        """Test non-200 status code handling."""
        mock_response = MagicMock()
        mock_response.status_code = 503
        mock_get.return_value = mock_response
        
        urls = ["https://data.gov.tw/dataset/177136"]
        headers = {"User-Agent": "test-agent"}
        
        result = find_download_links(urls, headers)
        
        assert isinstance(result, dict)


class TestDownloadAndExtractZip:
    """Test suite for download_and_extract_zip function."""

    @patch("src.task.e_crawling_traffic_accident.requests.get")
    def test_download_and_extract_failed_download(self, mock_get):
        """Test failed download handling."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            zip_dir = tmpdir_path / "zips"
            extract_dir = tmpdir_path / "extracted"
            zip_dir.mkdir(exist_ok=True)
            extract_dir.mkdir(exist_ok=True)
            
            mock_response = MagicMock()
            mock_response.status_code = 404
            mock_get.return_value = mock_response
            
            result = download_and_extract_zip(
                download_link="https://example.com/notfound.zip",
                zipfile_save_dir=zip_dir,
                zipfile_name="test_data.zip",
                unzipfile_save_dir=extract_dir
            )
            
            assert result is None or result == []

    @patch("src.task.e_crawling_traffic_accident.requests.get")
    def test_download_and_extract_timeout(self, mock_get):
        """Test download timeout handling."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            zip_dir = tmpdir_path / "zips"
            extract_dir = tmpdir_path / "extracted"
            zip_dir.mkdir(exist_ok=True)
            extract_dir.mkdir(exist_ok=True)
            
            mock_get.side_effect = requests.exceptions.Timeout()
            
            try:
                result = download_and_extract_zip(
                    download_link="https://example.com/file.zip",
                    zipfile_save_dir=zip_dir,
                    zipfile_name="test_data.zip",
                    unzipfile_save_dir=extract_dir
                )
                assert result is None or result == []
            except requests.exceptions.Timeout:
                pass
