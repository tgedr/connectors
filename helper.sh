#!/usr/bin/env bash
# ===> COMMON SECTION START  ===>
# http://bash.cumulonim.biz/NullGlob.html
shopt -s nullglob

# -------------------------------
# --- COMMON FUNCTION SECTION ---

debug(){
    local __msg="$@"
    echo " [DEBUG] `date` ... $__msg "
}

info(){
    local __msg="$@"
    echo " [INFO]  `date` ->>> $__msg "
}

warn(){
    local __msg="$@"
    echo " [WARN]  `date` *** $__msg "
}

err(){
    local __msg="$@"
    echo " [ERR]   `date` !!! $__msg "
}

# -------------------------------
# --- MAIN SECTION ---

if [ -z "$this_folder" ]; then
  this_folder="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
  if [ -z "$this_folder" ]; then
    this_folder=$(dirname $(readlink -f $0))
  fi
fi
parent_folder=$(dirname "$this_folder")

if [ -f "${this_folder}/.variables" ]; then
    debug "we have a '.variables' file"
    . "${this_folder}/.variables"
fi

if [ -f "${this_folder}/.secrets" ]; then
    debug "we have a '.secrets' file"
    . "${this_folder}/.secrets"
fi

usage()
{
  cat <<EOM
  usages:
  $(basename $0) {reqs}
                        reqs
                            install required packages to build and publish python packages
                        build
                            build package
                        test
                            runs unit tests
                        create_requirements
                            creates "requirements.txt" file
                        code_check
                          code check: runs 'black', 'autoflake' & 'isort'
                        publish {patch|minor|major}
                          after testing and committing everything, we may want to bumpversion,
                          tag and push the code to create e new package version


EOM
  exit 1
}

reqs()
{
    info "[reqs|in]"
    python -m pip install --upgrade pip setuptools wheel build twine artifacts-keyring keyring bump2version pipreqs && \
    python -m pip install astroid==2.5.2 pycodestyle==2.7.0 pyflakes==2.3.0 isort black autoflake pytest pytest-cov
    return_value="$?"
    info "[reqs|out] => ${return_value}"
    [[ ! "$return_value" -eq "0" ]] && exit 1
}

code_check()
{
    info "[code_check|in]"
    autoflake --in-place --remove-unused-variables --check -r src test && \
    isort -rc src test && \
    black src test -t py37 --line-length=120
    return_value="$?"
    if [ "$return_value" -eq "0" ]; then
      outdated_packages=$(pip list --outdated --format freeze  | wc -l | awk '{ print $1 }')
      if [ ! "$outdated_packages" -eq "0" ]; then
        err "[code_check] outdated packages: $outdated_packages"
        return_value="1"
      fi
    fi
    info "[code_check|out] => ${return_value}"
    [[ ! "$return_value" -eq "0" ]] && exit 1
}


build()
{
    info "[build|in]"
    rm -f dist/*
    pyproject-build && twine check dist/*
    return_value="$?"
    info "[build|out] => ${return_value}"
    [[ ! "$return_value" -eq "0" ]] && exit 1
}

publish()
{
    info "[publish|in] ($1)"
    bump2version --list "$1" && git push --follow-tags
    return_value="$?"
    info "[publish|out] => ${return_value}"
    [[ ! "$return_value" -eq "0" ]] && exit 1
}

test()
{
    info "[test|in]"
    python -m pytest --durations=0 --cov=src --junitxml=test-results.xml --cov-report=xml --cov-report=html
    return_value="$?"
    info "[test|out] => ${return_value}"
    [[ ! "$return_value" -eq "0" ]] && exit 1
}

create_requirements()
{
  info "[create_requirements|in]"
  pipreqs ./ --ignore .env --force
  info "[create_requirements|out]"
}

info "starting [ $0 $1 $2 $3 $4 ] ..."
_pwd=$(pwd)

case "$1" in
    reqs)
        reqs
        ;;
    build)
        build
        ;;
    publish)
        case "$2" in
            patch)
                publish "patch"
                ;;
            minor)
                publish "minor"
                ;;
            major)
                publish "major"
                ;;
            *)
                usage
        esac
        ;;
    test)
        test
        ;;
    create_requirements)
        create_requirements
        ;;
    code_check)
        code_check
        ;;
    *)
        usage
esac

info "...[ $0 $1 $2 $3 $4 ] done."