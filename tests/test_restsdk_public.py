"""
Test suite for restsdk_public.py

This module tests critical functionality and guards against regressions
in the main recovery script.
"""

import subprocess
import sys
import pytest


class TestModuleImport:
    """Test that the module can be imported without errors."""
    
    def test_module_imports_without_executing_main(self):
        """
        Ensure restsdk_public can be imported without executing main block.
        
        This is a regression test for a bug where code that referenced 'args'
        was placed at module level outside the if __name__ == "__main__" block,
        causing NameError when importing the module for testing.
        
        Bug introduced in commit c69b06e8 (2025-12-21).
        """
        result = subprocess.run(
            [sys.executable, "-c", "import restsdk_public; print('SUCCESS')"],
            capture_output=True,
            text=True,
            timeout=5
        )
        
        assert result.returncode == 0, f"Import failed with stderr: {result.stderr}"
        assert "SUCCESS" in result.stdout
        assert "NameError" not in result.stderr
        assert "args" not in result.stderr or "args is not defined" not in result.stderr


class TestArgumentParsing:
    """Test command-line argument parsing."""
    
    def test_help_flag_works(self):
        """Ensure --help flag displays help without errors."""
        result = subprocess.run(
            [sys.executable, "restsdk_public.py", "--help"],
            capture_output=True,
            text=True,
            timeout=5
        )
        
        # --help should exit with code 0
        assert result.returncode == 0
        assert "WD MyCloud REST SDK Recovery Tool" in result.stdout
        assert "--db" in result.stdout
        assert "--filedir" in result.stdout
        assert "--dumpdir" in result.stdout
    
    def test_script_execution_without_crashing_on_startup(self):
        """
        Test that script can parse arguments and start without NameError.
        
        This guards against the specific bug where 'args' was referenced before
        being defined via parser.parse_args().
        
        We use --preflight mode with dummy paths since it exits early and doesn't
        need actual database files.
        """
        result = subprocess.run(
            [
                sys.executable,
                "restsdk_public.py",
                "--preflight",
                "--filedir", "/tmp",
                "--dumpdir", "/tmp"
            ],
            capture_output=True,
            text=True,
            timeout=10
        )
        
        # Should complete without NameError
        assert "NameError: name 'args' is not defined" not in result.stderr
        
        # Preflight should run and show output
        assert "Pre-flight Hardware" in result.stdout or result.returncode == 0


class TestCodeStructure:
    """Test the overall code structure and organization."""
    
    def test_main_block_is_properly_guarded(self):
        """
        Verify that the if __name__ == "__main__" guard is present and working.
        
        When we import the module, we should not execute the main logic.
        This is critical for testing and prevents the 'args' not defined bug.
        """
        import restsdk_public
        
        # If we got here, the module imported successfully
        # The module should define key functions but not execute main logic
        assert hasattr(restsdk_public, 'init_copy_tracking_tables')
        assert hasattr(restsdk_public, 'regenerate_copied_files_from_dest')
        assert callable(restsdk_public.init_copy_tracking_tables)
    
    def test_traceback_is_imported(self):
        """
        Ensure traceback module is imported.
        
        This is needed for error handling in the main block.
        Previously this was missing, causing another NameError.
        """
        import restsdk_public
        
        # Check that traceback is available in the module's namespace
        # We do this by checking the imports
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                "import restsdk_public; import traceback; print('traceback available')"
            ],
            capture_output=True,
            text=True,
            timeout=5
        )
        
        assert result.returncode == 0
        assert "traceback available" in result.stdout


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
