use anyhow::Result;

pub(crate) fn cmd_hash_password() -> Result<()> {
    let password = rpassword_read()?;
    println!("{}", hash_password_value(&password)?);
    Ok(())
}

pub(crate) fn hash_password_value(password: &str) -> Result<String> {
    if password.is_empty() {
        anyhow::bail!("password must not be empty");
    }
    Ok(bcrypt::hash(password, bcrypt::DEFAULT_COST)
        .map_err(|e| anyhow::anyhow!("bcrypt hash failed: {}", e))?)
}

fn rpassword_read() -> Result<String> {
    eprint!("Enter password: ");
    let password = if atty_stdin() {
        read_password_from_tty()
    } else {
        let mut line = String::new();
        std::io::stdin().read_line(&mut line)?;
        line.trim_end_matches('\n')
            .trim_end_matches('\r')
            .to_string()
    };
    eprintln!();
    Ok(password)
}

fn atty_stdin() -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::io::AsRawFd;
        unsafe { libc_isatty(std::io::stdin().as_raw_fd()) }
    }
    #[cfg(not(unix))]
    {
        false
    }
}

#[cfg(unix)]
fn libc_isatty(fd: std::os::unix::io::RawFd) -> bool {
    unsafe extern "C" {
        fn isatty(fd: i32) -> i32;
    }
    unsafe { isatty(fd) != 0 }
}

fn read_password_from_tty() -> String {
    #[cfg(unix)]
    {
        use std::io::Read;

        if let Ok(mut tty) = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open("/dev/tty")
        {
            disable_echo(&tty);
            let mut buf = String::new();
            let _ = tty.read_to_string(&mut buf);
            enable_echo(&tty);
            return buf.lines().next().unwrap_or("").to_string();
        }
    }

    let mut line = String::new();
    let _ = std::io::stdin().read_line(&mut line);
    line.trim_end_matches('\n')
        .trim_end_matches('\r')
        .to_string()
}

#[cfg(unix)]
fn disable_echo(_file: &std::fs::File) {}

#[cfg(unix)]
fn enable_echo(_file: &std::fs::File) {}
