from .core import *  # noqa: F401,F403

@app.route('/')
def home():
    if 'admin_id' not in session:
        return redirect(url_for('login'))
    return render_template('home.html', admin_id=session['admin_id'])

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        admin_id = request.form.get('admin_id')
        if admin_id in ADMIN_IDS:
            session['admin_id'] = admin_id
            return redirect(url_for('home'))
        else:
            return render_template('login.html', error="无效的管理员ID")
    return render_template('login.html')

@app.route('/logout')
def logout():
    session.pop('admin_id', None)
    return redirect(url_for('login'))

@app.route('/update')
def update():
    if 'admin_id' not in session:
        return redirect(url_for('login'))
    return render_template('update.html')

@app.route('/check_update')
def check_update():
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})
    
    try:
        latest_release, message = update_manager.check_update()
        
        if latest_release:
            return jsonify({
                "success": True,
                "update_available": True,
                "current_version": update_manager.current_version,
                "latest_version": latest_release['tag_name'],
                "release_name": latest_release['name'],
                "published_at": latest_release['published_at'],
                "changelog": latest_release['body'],
                "message": message
            })
        else:
            return jsonify({
                "success": True,
                "update_available": False,
                "current_version": update_manager.current_version,
                "message": message
            })
            
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

@app.route('/get_releases')
def get_releases():
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})
    
    try:
        releases = update_manager.get_latest_releases(10)
        
        return jsonify({
            "success": True,
            "releases": releases,
            "current_version": update_manager.current_version
        })
        
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

@app.route('/perform_update', methods=['POST'])
def perform_update():
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})
    
    try:
        data = request.get_json()
        release_tag = data.get('release_tag')
        
        if not release_tag:
            return jsonify({"success": False, "error": "未指定release标签"})
        
        success, message = update_manager.perform_update_with_backup(release_tag)
        
        return jsonify({
            "success": success,
            "message": message
        })
        
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

@app.route('/get_backups')
def get_backups():
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})
    
    try:
        backups = update_manager.get_backups()
        return jsonify({
            "success": True,
            "backups": backups
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})
