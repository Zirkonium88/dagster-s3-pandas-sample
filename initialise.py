from datetime import date
from http import HTTPStatus
from os import link, rename, getenv
from pathlib import Path
import platform
from shlex import split
from subprocess import run
from requests import get, post

git_url_https = "https://git.we.decodeinsurance.de/mdp/"
git_url_ssh = "git@git.we.decodeinsurance.de:mdp/"
git_mdp_group_id = "69"
git_cicd_project_id = "167"
gitlab_api = "https://git.we.decodeinsurance.de/api/v4/"
gitlab_projects_api = gitlab_api + "projects/"
gitlab_groups_api_mdp = gitlab_api + "groups/" + git_mdp_group_id + "/projects"
gitlab_api_token_environment_variable = getenv("GITLAB_API_TOKEN")
operating_system = platform.system()


def create_venv(operating_system):
    """Creates a virtual environment and installs required dependencies in a subprocess.

    Args:
        operating_system: The name of the operating system
    """
    print("Creating virtual environment and installing required dependencies...")
    if operating_system == "Windows":
        run(split(
            "python -m venv .venv && cd .venv/Scripts && activate && cd.. && cd.. && pip install -r requirements.txt"),
            shell=True).check_returncode()
    else:
        run(split("bash -c 'python3 -m venv .venv && source .venv/bin/activate && pip3 install -r requirements.txt'")) \
            .check_returncode()


def input_authentication_method():
    """Returns user input of the desired authentication method.

    Returns:
        Potentially incorrect authentication method from user input
    """
    return input("Do you want to use HTTPS or SSH for authentication? ").lower()


def check_input_authentication_method(authentication_method):
    """Verifies authentication method from user input.

     Args:
         authentication_method: Authentication method input by user
     Returns:
         https or ssh
     """
    while authentication_method not in ["https", "ssh"]:
        authentication_method = input_authentication_method()
    return authentication_method


def determine_git_url(authentication_method):
    """Constructs the GitLab URL depending on the authentication method.

    Args:
          authentication_method: Authentication method (https or ssh)
    Returns:
         GitLab URL dependent on the authentication method
    """
    if authentication_method == "https":
        return git_url_https
    elif authentication_method == "ssh":
        return git_url_ssh
    else:
        raise Exception("Invalid authentication method")


def input_gitlab_personal_access_token():
    """Returns user input of a GitLab Personal access token.

    Returns:
        Potentially incorrect access token from user input
    """
    return input("Enter your GitLab Personal Access Token: ")


def verify_gitlab_personal_token(gitlab_personal_token):
    """Verifies a GitLab Personal access token from user input.

    Args:
        gitlab_personal_token: GitLab Personal access token to be verified
    Returns:
        Valid Gitlab Personal access token
    """
    while get(
            headers={"private-token": gitlab_personal_token},
            url=gitlab_api + "version/"
    ).status_code == HTTPStatus.UNAUTHORIZED:  # 401
        print("Invalid access token")
        return verify_gitlab_personal_token(input_gitlab_personal_access_token())
    return gitlab_personal_token


def get_mdp_projects(page, gitlab_personal_token):
    """Use GitLab API to get projects from MDP group.

    Args:
        page: Projects are divided into pages that can be iterated
        gitlab_personal_token: Valid GitLab Personal access token
    Returns:
        Simple information on projects (e.g. owner, name) in MDP group
    """
    return get(
        headers={"private-token": gitlab_personal_token},
        url=gitlab_groups_api_mdp,
        params={
            "page": str(page),
            "simple": True
        }).json()


def get_mdp_projects_names(gitlab_personal_token):
    """Iterates all projects from MDP group and collects their names.

    Args:
        gitlab_personal_token: Valid GitLab personal access token
    Returns:
        A list with all names of GitLab projects in MDP group
    """
    projects = list()
    page=1
    current_projects = get_mdp_projects(page, gitlab_personal_token)
    while current_projects:
        projects += current_projects
        page += 1
        current_projects = get_mdp_projects(page, gitlab_personal_token)
    return [project["name"] for project in projects]


def input_app_name():
    """Returns user input of a name for the new project.

    Returns:
        Potentially already existing app name from user input
    """
    return input("Enter the name of your new CDK App: ")


def verify_input_app_name(app_name, gitlab_personal_token):
    """Verifies the name of the new project from user input.

    Args:
        app_name: New name to be checked for a duplicate
        gitlab_personal_token: Valid GitLab Personal access token
    Returns:
        Valid app name
    """
    mdp_projects_names = get_mdp_projects_names(gitlab_personal_token)
    while app_name in mdp_projects_names:
        print("This app name is already in use! Please enter another name.")
        app_name = input_app_name()
    return app_name


def input_project_visibility():
    """Returns user input of the project visibility for the new project.

    Returns:
        Potentially invalid project visibility from user input
    """
    return input("Project visibility (private or internal): ").lower()


def check_input_project_visibility(project_visibility):
    """Returns a valid project visibility.

    Args:
        project_visibility: Project visibility to be validated
    Returns:
        private or internal
    """
    while project_visibility not in ["private", "internal"]:
        project_visibility = input_project_visibility()
    return project_visibility


def input_project_description():
    """Returns user input of the project description for the new project.

    Returns:
        Possibly empty project description from user input
    """
    return input("Project description: ")


def create_new_remote_project(app_name, project_visibility, project_description, gitlab_personal_token):
    """Use GitLab API to create new project in MDP group. Raises an exception if creation was unsuccessful.

    Args:
        app_name: Validated app name
        project_visibility: Validated project visibility
        project_description: Project description (may be empty)
        gitlab_personal_token: Valid GitLab Personal access token
    """
    post(
        url=gitlab_projects_api,
        headers={"private-token": gitlab_personal_token},
        params={
            "name": app_name,
            "namespace_id": git_mdp_group_id,
            "visibility": project_visibility,
            "description": project_description
        }
    ).raise_for_status()
    print("Created new remote project in GitLab!")


def input_cicd_branch():
    """Returns user input of the branch to checkout from the cicd submodule.

    Returns:
        Potentially non-existing branch from user input
    """
    return input("Enter branch of the CI/CD pipeline to checkout as a submodule: ")


def get_cicd_branches_names(gitlab_personal_token):
    """Use GitLab API to get all branches from cicd project in MDP group.

    Args:
        gitlab_personal_token: Valid GitLab Personal access token
    Returns:
        A list of every branch in the cicd project
    """
    return list(
        branch["name"] for branch in get(
            headers={"private-token": gitlab_personal_token},
            url=gitlab_projects_api + git_cicd_project_id + "/repository/branches",
            params={"per_page": "100"}
        ).json()
    )


def verify_branch_cicd_submodule(gitlab_personal_token, cicd_branch):
    """Verifies the given branch of the cicd project.

    Args:
          gitlab_personal_token: Valid GitLab Personal access token
          cicd_branch: Branch of cicd project to be validated
    Returns:
        A valid branch of the cicd project that can be checked out
    """
    cicd_branches_names = get_cicd_branches_names(gitlab_personal_token)
    while cicd_branch not in cicd_branches_names:
        print("Available branches: " + str(cicd_branches_names))
        cicd_branch = input_cicd_branch()
    return cicd_branch


def checkout_cicd_submodule(git_url_cicd, gitlab_personal_token, cicd_branch):
    """ Runs a subprocess to add the cicd project as Git submodule.

    Args:
        git_url_cicd: Valid URL of the cicd project
        gitlab_personal_token: Valid GitLab Personal access token
        cicd_branch: Potentially invalid branch of the cicd project to checkout
    """
    cicd_branch = verify_branch_cicd_submodule(gitlab_personal_token, cicd_branch)
    run(["git", "submodule", "add", "-b", cicd_branch, git_url_cicd]).check_returncode()
    return cicd_branch


def replace_text(to_be_replaced, replacement: str, file):
    """Utility method for replacing text in a given file.

    Args:
        to_be_replaced: Text that shall be replaced
        replacement: Text to replace to_be_replaced with
        file: Path to the file that contains the text
    """
    file.write_text(file.read_text().replace(to_be_replaced, replacement))


def configure_project(operating_system, app_name, git_url_cicd, cicd_branch):
    """Performs project-specific configurations."""
    app_name_underscore = app_name.replace("-", "_")
    cicd_build_path = ".idea/runConfigurations/cicd_build.xml"
    if operating_system != "Windows":
        replace_text("$PROJECT_DIR$\\.venv\\Scripts\\python.exe", "$PROJECT_DIR$/.venv/bin/python", Path(cicd_build_path))
    for path in [cicd_build_path, ".gitlab-ci.yml", "app.py", "tests/unit/test_sample_stack.py"]:
        replace_text("sample-repo", app_name, Path(path))
    for path in ["app.py", "tests/unit/test_sample_stack.py"]:
        replace_text("sample_repo", app_name_underscore, Path(path))
    replace_text("dmz", cicd_branch, Path(".gitlab-ci.yml"))
    replace_text("123456", input("Enter HDI Personal number: "), Path("app.py"))
    rename("sample_repo", app_name_underscore)
    rename("README.md", "README_SAMPLE_REPO.md")
    rename("README_CDK.md", "README.md")
    replace_text(git_url_cicd, "../cicd", Path(".gitmodules"))
    link(Path("cicd", "loggerconfig.yml"), "loggerconfig.yml")


def push_to_new_remote_project(git_url, app_name):
    """Runs multiple Git commands in a subprocess to push the configured project to the newly created GitLab project.

    Args:
        git_url: GitLab URL based on the authentication method
        app_name: Name of the new project
    """
    new_remote = git_url + app_name + ".git"
    change_remote = ["git", "remote", "set-url", "origin", new_remote]
    rename_main_branch_to_oldmain = ["git", "branch", "-m", "main", "oldmain"]
    checkout_new_orphan_branch = ["git", "checkout", "--orphan", "master"]
    delete_oldmain_branch = ["git", "branch", "-D", "oldmain"]
    add_all_files = ["git", "add", "-A"]
    remove_initialise_py = ["git", "rm", "--cached", "initialise.py", ".idea/runConfigurations/initialise.xml",
                            "README_SAMPLE_REPO.md"]
    initial_commit = ["git", "commit", "-m", "Initial commit"]
    push_to_new_repository = ["git", "push", "-u", "origin", "master"]
    for command in [change_remote, rename_main_branch_to_oldmain, checkout_new_orphan_branch, delete_oldmain_branch,
                    add_all_files, remove_initialise_py, initial_commit, push_to_new_repository]:
        run(command)


def checkout_new_feature_branch(feature_branch_name):
    """Checks out a new feature branch.

    Args:
        feature_branch_name: Name of the feature branch (usually the current date)
    """
    run(["git", "checkout", "-b", "feature/" + feature_branch_name])


if __name__ == '__main__':
    create_venv(operating_system)
    git_url = determine_git_url(check_input_authentication_method(input_authentication_method()))
    git_url_cicd = git_url + "cicd.git"
    gitlab_personal_token = verify_gitlab_personal_token(gitlab_api_token_environment_variable)
    app_name_input = verify_input_app_name(input_app_name(), gitlab_personal_token)
    project_visibility = check_input_project_visibility(input_project_visibility())
    create_new_remote_project(app_name_input, project_visibility, input_project_description(), gitlab_personal_token)
    cicd_branch = checkout_cicd_submodule(git_url_cicd, gitlab_personal_token, input_cicd_branch())
    configure_project(operating_system, app_name_input, git_url_cicd, cicd_branch)
    push_to_new_remote_project(git_url, app_name_input)
    checkout_new_feature_branch(date.today().strftime("%d%m%y"))
    print("Successfully initialised new project!")
    print("Link to your new repository: " + git_url_https + app_name_input)
